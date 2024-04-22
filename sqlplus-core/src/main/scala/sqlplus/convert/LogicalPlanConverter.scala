package sqlplus.convert

import com.google.common.collect.BoundType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.util.{NlsString, Sarg}
import sqlplus.catalog.CatalogManager
import sqlplus.expression._
import sqlplus.ghd.GhdAlgorithm
import sqlplus.graph._
import sqlplus.gyo.GyoAlgorithm
import sqlplus.plan.table.SqlPlusTable
import sqlplus.types._
import sqlplus.utils.DisjointSet

import java.util.GregorianCalendar
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Convert logical plan to join trees. Then construct a ComparisonHyperGraph for each join tree.
 * Select the join tree and its ComparisonHyperGraph with minimum degree.
 *
 */
class LogicalPlanConverter(val variableManager: VariableManager, val catalogManager: CatalogManager) {
    val gyo: GyoAlgorithm = new GyoAlgorithm
    val ghd: GhdAlgorithm = new GhdAlgorithm

    def isAcyclic(relationalHyperGraph: RelationalHyperGraph): Boolean = {
        gyo.isAcyclic(relationalHyperGraph)
    }

    def break(relationalHyperGraph: RelationalHyperGraph): Option[List[(RelationalHyperGraph, List[ExtraCondition])]] = {
        // a query is breakable if:
        // 1. Primary Key is nonempty for all relations
        // 2. There is only one node with 0 in-degree (the fact table)
        // 3. All the relations are reachable from the fact table
        // 4. Starting from the fact table, there are more than 1 paths that meet at the same table (e.g., nation in TPCH-Q5)
        if (relationalHyperGraph.getEdges().exists(r => r.getPrimaryKeys().isEmpty)) {
            // cond 1 unsatisfied
            return None
        }

        // find the fact table (if any)
        val reachable: mutable.HashMap[Relation, mutable.HashSet[Relation]] = mutable.HashMap.empty
        val inDegree: mutable.HashMap[Relation, Int] = mutable.HashMap.empty
        relationalHyperGraph.getEdges().foreach(r => inDegree(r) = 0)

        for {
            r1 <- relationalHyperGraph.getEdges()
            r2 <- relationalHyperGraph.getEdges()
            if r1.relationId < r2.relationId
        } {
            val overlap = r1.getNodes().intersect(r2.getNodes())
            if (overlap.nonEmpty) {
                if (r1.getPrimaryKeys().subsetOf(overlap)) {
                    reachable.getOrElseUpdate(r2, mutable.HashSet.empty[Relation]).add(r1)
                    inDegree(r1) = inDegree(r1) + 1
                } else if (r2.getPrimaryKeys().subsetOf(overlap)) {
                    reachable.getOrElseUpdate(r1, mutable.HashSet.empty[Relation]).add(r2)
                    inDegree(r2) = inDegree(r2) + 1
                }
            }
        }

        if (inDegree.count(t => t._2 == 0) != 1) {
            // cond 2 unsatisfied
            return None
        }

        val fact = inDegree.find(t => t._2 == 0).get._1
        val queue = mutable.Queue.empty[Relation]
        val seen = mutable.Set.empty[Relation]
        val meet = mutable.Set.empty[Relation]
        queue.enqueue(fact)

        while (queue.nonEmpty) {
            val head = queue.dequeue()
            if (!seen.contains(head)) {
                seen.add(head)
                if (reachable.contains(head)) {
                    reachable(head).foreach(r => queue.enqueue(r))
                }
            } else {
                meet.add(head)
            }
        }

        if (seen.size < relationalHyperGraph.getEdges().size) {
            // cond 3 unsatisfied
            return None
        }

        if (meet.size > 1 || meet.isEmpty) {
            // currently, only one meet is supported
            return None
        }

        val parents = relationalHyperGraph.getEdges().filter(r => reachable.contains(r) && reachable(r).contains(meet.head))
        val results = parents.map(p => {
            // keep the join between p and meet
            // break the join between other relations and meet by replacing the join variable with a dummy variable
            // each choice of p results in a different relationalHyperGraph and extraEqualConditions
            val others = parents - p
            val meetVariables = meet.head.getVariableList().toSet
            var updatedHyperGraph = relationalHyperGraph
            val extraEqualConditions = ListBuffer.empty[ExtraCondition]

            others.foreach(o => {
                updatedHyperGraph = updatedHyperGraph.removeHyperEdge(o)
                val intersect = meetVariables.intersect(o.getVariableList().toSet)
                val replace = intersect.map(v => (v, variableManager.getNewVariable(v.dataType)))
                val newHyperEdge = o.replaceVariables(replace.toMap)
                updatedHyperGraph = updatedHyperGraph.addHyperEdge(newHyperEdge)
                extraEqualConditions.appendAll(replace.map(t => ExtraEqualToCondition(SingleVariableExpression(t._1), SingleVariableExpression(t._2))))
            })

            (updatedHyperGraph, extraEqualConditions.toList)
        })
        Some(results.toList)
    }

    def tryFixRoot(relationalHyperGraph: RelationalHyperGraph, isAggregation: Boolean, groupByVariables: Set[Variable]): List[Relation] = {
        // candidate relation(s): size >= factor * largest relation size
        val factor = 0.8

        // we can fix the root of a query if
        // 1. It is an aggregation query
        // 2. Every relation have cardinality > 0
        // 3. Every relation have nonempty Primary Key
        // 4. The candidate relation does not contain any group by variables
        //  (the plan will be returned by GYO without fix root if the largest relation contains some group by variables)
        // 5. The variables in the candidate relation determine the group by variables
        if (!isAggregation || relationalHyperGraph.getEdges().exists(r => r.getCardinality() <= 0 || r.getPrimaryKeys().isEmpty))
            return List()

        val largestCardinality = relationalHyperGraph.getEdges().map(r => r.getCardinality()).max
        val largestRelations = relationalHyperGraph.getEdges().filter(r => r.getCardinality() >= largestCardinality * factor)

        val candidateRelations = largestRelations.filter(r => r.getNodes().intersect(groupByVariables).isEmpty)

        val chaseVariables = mutable.HashMap.empty[Relation, Set[Variable]]
        relationalHyperGraph.getEdges().foreach(r => {
            chaseVariables(r) = r.getNodes()
        })

        def loop(): Unit = {
            for {
                r1 <- relationalHyperGraph.getEdges()
                r2 <- relationalHyperGraph.getEdges()
                if r1 != r2
            } {
                if (r1.getPrimaryKeys().subsetOf(chaseVariables(r2)) && !chaseVariables(r1).subsetOf(chaseVariables(r2))) {
                    chaseVariables(r2) = chaseVariables(r2).union(chaseVariables(r1))
                    return loop()
                } else if (r2.getPrimaryKeys().subsetOf(chaseVariables(r1)) && !chaseVariables(r2).subsetOf(chaseVariables(r1))) {
                    chaseVariables(r1) = chaseVariables(r1).union(chaseVariables(r2))
                    return loop()
                }
            }
        }

        loop()
        candidateRelations.filter(r => chaseVariables(r).containsAll(groupByVariables)).toList
    }

    def runGyo(relationalHyperGraph: RelationalHyperGraph, fixRootEnable: Boolean, pruneEnable: Boolean,
               aggregations: List[(Variable, String, List[Expression])],
               groupByVariables: Set[Variable], topVariables: Set[Variable]): (List[(JoinTree, RelationalHyperGraph, List[ExtraCondition])], Boolean) = {
        // first run GYO without fixRoot
        val gyoResult = gyo.run(relationalHyperGraph, topVariables, pruneEnable)
        val withoutFixRoot = (gyoResult.candidates.map(t => (t._1, t._2, List.empty)), gyoResult.isFreeConnex)

        // if fixRoot is set, try to fix the root as the largest relation
        if (fixRootEnable) {
            val optFixRoot = tryFixRoot(relationalHyperGraph, aggregations.nonEmpty, groupByVariables)
            if (optFixRoot.nonEmpty) {
                optFixRoot.foldLeft(withoutFixRoot)((z, r) => {
                    val gyoResult = gyo.runWithFixRoot(relationalHyperGraph, r, pruneEnable)
                    val withFixRoot = (gyoResult.candidates.map(t => (t._1, t._2, List.empty)), gyoResult.isFreeConnex)
                    (z._1 ++ withFixRoot._1, z._2)
                })
            } else {
                // unable to fix root
                withoutFixRoot
            }
        } else {
            // fix root disabled
            withoutFixRoot
        }
    }

    def run(root: RelNode, fixRootEnable: Boolean = false, pruneEnable: Boolean = true): RunResult = {
        val context = traverseLogicalPlan(root)
        val relations = context.relations
        val conditions = context.conditions
        val outputVariables = context.outputVariables
        val requiredVariables = context.requiredVariables
        val computations = context.computations.toList
        val isFull = context.isFull
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val optTopK = context.optTopK
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables

        val (candidatesFromResults, isFreeConnex) = if (isAcyclic(relationalHyperGraph)) {
            // for acyclic queries, run GYO Algorithm
            runGyo(relationalHyperGraph, fixRootEnable, pruneEnable, aggregations, groupByVariables.toSet, topVariables)
        } else {
            // try to break cyclic queries
            val optBreakResult = break(relationalHyperGraph)
            if (optBreakResult.nonEmpty) {
                // for breakable cyclic queries, break and then run GYO Algorithm
                val hyperGraphAndExtraConditions = optBreakResult.get
                val runGyoResult = hyperGraphAndExtraConditions.map(t => {
                    val hyperGraph = t._1
                    val extraConditions = t._2
                    val (candidates, isFreeConnex) =  runGyo(hyperGraph, fixRootEnable, pruneEnable, aggregations, groupByVariables.toSet, topVariables)
                    val candidatesWithExtraConditions = candidates.map(t => (t._1, t._2, t._3 ++ extraConditions))
                    (candidatesWithExtraConditions, isFreeConnex)
                })
                // if free-connex is possible, return the free connex candidates only
                val isFreeConnexPossible = runGyoResult.exists(t => t._2)
                if (isFreeConnexPossible) {
                    (runGyoResult.filter(t => t._2).flatMap(t => t._1), true)
                } else {
                    (runGyoResult.flatMap(t => t._1), false)
                }
            } else {
                // for unbreakable cyclic queries, run GHD Algorithm
                val ghdResult = ghd.run(relationalHyperGraph, topVariables)
                // There is no extra condition in GHD plans
                (ghdResult.candidates.map(t => (t._1, t._2, List.empty)), false)
            }
        }

        val candidates: List[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])] = candidatesFromResults.flatMap(t => {
            val joinTree = t._1
            val hyperGraph = t._2
            val extraEqualConditions = ListBuffer.empty[ExtraCondition]
            extraEqualConditions.appendAll(t._3)

            val comparisonHyperEdges = ListBuffer.empty[Comparison]
            conditions.foreach {
                case LessThanCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator("<", leftOperand, rightOperand, false)
                    comparisonHyperEdges.append(Comparison(path, op, leftOperand, rightOperand))
                case LessThanOrEqualToCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator("<=", leftOperand, rightOperand, false)
                    comparisonHyperEdges.append(Comparison(path, op, leftOperand, rightOperand))
                case GreaterThanCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator(">", leftOperand, rightOperand, false)
                    comparisonHyperEdges.append(Comparison(path, op, leftOperand, rightOperand))
                case GreaterThanOrEqualToCondition(leftOperand, rightOperand) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, leftOperand, rightOperand).toSet
                    val op = Operator.getOperator(">=", leftOperand, rightOperand, false)
                    comparisonHyperEdges.append(Comparison(path, op, leftOperand, rightOperand))
                case EqualToLiteralCondition(operand, literal) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("=", operand, literal, false)
                    comparisonHyperEdges.append(Comparison(path, op, operand, operand))
                case NotEqualToLiteralCondition(operand, literal) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("<>", operand, literal, false)
                    comparisonHyperEdges.append(Comparison(path, op, operand, operand))
                case LikeCondition(operand, s, isNeg) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("LIKE", operand, s, isNeg)
                    comparisonHyperEdges.append(Comparison(path, op, operand, operand))
                case InCondition(operand, literals, isNeg) =>
                    val path = getShortestInRelationalHyperGraph(hyperGraph.getEdges(), joinTree, operand, operand).toSet
                    val op = Operator.getOperator("IN", operand, literals, isNeg)
                    comparisonHyperEdges.append(Comparison(path, op, operand, operand))
                case OrCondition(conditions, involved) =>
                    val involvedVariables = involved.flatMap(e => e.getVariables())
                    val optPushedDownRelation = relations.find(r => r.isInstanceOf[TableScanRelation] && involvedVariables.subsetOf(r.getNodes()))
                    if (optPushedDownRelation.nonEmpty) {
                        val path = new JoinTreeEdge(optPushedDownRelation.get, optPushedDownRelation.get)
                        val op = OrOperator(conditions)
                        comparisonHyperEdges.append(Comparison(Set(path), op, DummyExpression, DummyExpression))
                    } else {
                        extraEqualConditions.append(ExtraOrCondition(conditions))
                    }
                case ex: ExtraCondition =>
                    extraEqualConditions.append(ex)
            }

            val comparisonHyperGraph = new ComparisonHyperGraph(comparisonHyperEdges.toSet)

            // joinTreesWithComparisonHyperGraph is a candidate(for selection of minimum degree)
            // only when the comparisonHyperGraph is berge-acyclic
            if (comparisonHyperGraph.isBergeAcyclic())
                List((joinTree, comparisonHyperGraph, extraEqualConditions.toList))
            else
                List.empty
        })

        RunResult(candidates, outputVariables, computations, isFull, isFreeConnex, groupByVariables, aggregations, optTopK)
    }

    def candidatesWithLimit(list: List[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])], limit: Int): List[(JoinTree, ComparisonHyperGraph, List[ExtraCondition])] = {
        val zippedWithDegree = list.map(t => (t._1, t._2, t._3, t._2.getDegree()))
        val minDegree = zippedWithDegree.map(t => t._4).min
        zippedWithDegree.filter(t => t._4 == minDegree).take(limit).map(t => (t._1, t._2, t._3))
    }

    def runAndSelect(root: RelNode, orderBy: String = "degree", desc: Boolean = true, limit: Int = 1, fixRootEnable: Boolean, pruneEnable: Boolean): RunResult = {
        val runResult = run(root, fixRootEnable, pruneEnable)

        val selected = orderBy match {
            case "degree" if !desc =>
                // select the joinTree and ComparisonHyperGraph with minimum degree
                runResult.candidates.sortBy(t => t._2.getDegree()).take(limit)
            case "fanout" if !desc =>
                // select the joinTree and ComparisonHyperGraph with minimum maxFanout
                runResult.candidates.sortBy(t => t._1.getMaxFanout()).take(limit)
            case "pk2fk" if !desc =>
                runResult.candidates.sortBy(t => t._1.pk2fkCount).take(limit)
            case "pk2fk" if desc =>
                runResult.candidates.sortBy(t => t._1.pk2fkCount).reverse.take(limit)
            case "fk2pk" if !desc =>
                runResult.candidates.sortBy(t => t._1.fk2pkCount).take(limit)
            case "fk2pk" if desc =>
                runResult.candidates.sortBy(t => t._1.fk2pkCount).reverse.take(limit)
            case "" =>
                runResult.candidates.take(limit)
        }

        RunResult(selected, runResult.outputVariables, runResult.computations, runResult.isFull, runResult.isFreeConnex,
            runResult.groupByVariables, runResult.aggregations, runResult.optTopK)
    }

    def traverseLogicalPlan(node: RelNode): Context = {
        node match {
            case logicalSort: LogicalSort => visitTopK(logicalSort)
            case logicalAggregate: LogicalAggregate => visitAggregation(logicalAggregate)
            case logicalProject: LogicalProject if logicalProject.getInput.isInstanceOf[LogicalAggregate] => visitAggregation(logicalProject)
            case logicalProject: LogicalProject => visitJoins(logicalProject)
        }
    }

    def visitTopK(logicalSort: LogicalSort): Context = {
        // TopK RelNode Structure:
        // LogicalSort -> Aggregation/Joins
        val context = logicalSort.getInput match {
            case logicalProject: LogicalProject => visitJoins(logicalProject)
            // TODO: handle TopK over Aggregations
        }

        val isDesc = logicalSort.getCollation.getFieldCollations.get(0).getDirection.isDescending
        val sortBy = logicalSort.getCollation.getFieldCollations.get(0).getFieldIndex
        val limit = logicalSort.fetch.asInstanceOf[RexLiteral].getValue.asInstanceOf[java.math.BigDecimal].intValue()
        context.optTopK = Some(TopK(context.outputVariables(sortBy), isDesc, limit))

        context
    }

    def visitAggregation(relNode: RelNode): Context = {
        // Aggregation RelNode Structure:
        // Option[LogicalProject] -> LogicalAggregate -> Joins
        val (optLogicalProject, logicalAggregate) = relNode match {
            case project: LogicalProject => (Some(project), project.getInput.asInstanceOf[LogicalAggregate])
            case aggregate: LogicalAggregate => (None, aggregate)
        }
        val context = visitJoins(logicalAggregate.getInput)

        val groupByVariables = logicalAggregate.getGroupSet.asList().map(_.toInt).map(i => context.outputVariables(i)).toList
        val aggregations = ListBuffer.empty[(Variable, String, List[Expression])]
        logicalAggregate.getAggCallList.toList.foreach(c => {
            val aggregateFunction = c.getAggregation.getName
            val aggregateArguments = c.getArgList.toList.map(i => context.computations.getOrElse(context.outputVariables(i), SingleVariableExpression(context.outputVariables(i))))
            val variable = variableManager.getNewVariable(DataType.fromSqlType(c.getType.getSqlTypeName))
            context.dependingVariables(variable) = c.getArgList.toList.flatMap(i => context.dependingVariables(context.outputVariables(i))).toSet
            val triple = (variable, aggregateFunction, aggregateArguments)
            aggregations.append(triple)
        })
        // the output of logicalAggregate is always groupByVariables followed by aggregation columns
        val aggregateOutputVariables = groupByVariables ++ aggregations.map(t => t._1)

        val computations = mutable.HashMap.empty[Variable, Expression]  // additional computations(in LogicalProject)
        if (optLogicalProject.isEmpty) {
            context.outputVariables = aggregateOutputVariables
            context.requiredVariables = aggregations.map(t => t._1).flatMap(v => context.dependingVariables(v)).toSet
        } else {
            val outputVariables = ListBuffer.empty[Variable]
            optLogicalProject.get.getProjects.toList.foreach({
                case call: RexCall =>
                    val expr = convertRexCallToExpression(call, aggregateOutputVariables)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(call.getType.getSqlTypeName))
                    computations(variable) = expr
                    context.dependingVariables(variable) = expr.getVariables().flatMap(v => context.dependingVariables(v))
                    outputVariables.append(variable)
                case inputRef: RexInputRef =>
                    val variable = aggregateOutputVariables(inputRef.getIndex)
                    outputVariables.append(variable)
                case literal: RexLiteral =>
                    val expr = convertRexLiteralToExpression(literal)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(literal.getType.getSqlTypeName))
                    computations(variable) = expr
                    context.dependingVariables(variable) = Set()
                    outputVariables.append(variable)
            })
            context.outputVariables = outputVariables.toList
            context.requiredVariables = (context.outputVariables.flatMap(v => context.dependingVariables(v)).toSet -- groupByVariables)
            context.computations = context.computations ++ computations.toMap
        }
        context.groupByVariables = groupByVariables
        context.aggregations = aggregations.toList

        context
    }

    def visitJoins(relNode: RelNode): Context = {
        // Joins RelNode Structure:
        // Option[LogicalProject] -> LogicalFilter -> LogicalJoin/LogicalAggregate/LogicalTableScan
        val (optLogicalProject, logicalFilter) = relNode match {
            case project: LogicalProject => (Some(project), project.getInput.asInstanceOf[LogicalFilter])
            case filter: LogicalFilter => (None, filter)
        }

        val condition = logicalFilter.getCondition.asInstanceOf[RexCall]
        assert(condition.getOperator.getName != "OR")
        val operands = (if (condition.getOperator.getName == "AND") condition.getOperands.toList else List(condition))
            .filter(n => !n.isInstanceOf[RexLiteral] || n.asInstanceOf[RexLiteral].getValue.toString != "true")
        assert(operands.stream.allMatch((operand: RexNode) => operand.isInstanceOf[RexCall]))

        val variableTableBuffer = new ArrayBuffer[Variable]()
        val disjointSet = new DisjointSet[Variable]()
        for (field <- logicalFilter.getInput.getRowType.getFieldList) {
            val fieldType = DataType.fromSqlType(field.getType.getSqlTypeName)
            val newVariable = variableManager.getNewVariable(fieldType)
            disjointSet.makeNewSet(newVariable)
            variableTableBuffer.append(newVariable)
        }
        val variableTable = variableTableBuffer.toArray
        val conditionRexCalls = ListBuffer.empty[(RexCall, Boolean)]    // List[(rexCall, isNeg)]

        for (operand <- operands) {
            processOperandInCondition(operand, variableTable, disjointSet, false, conditionRexCalls)
        }

        for (i <- variableTable.indices) {
            // replace variables in variableTable with their representatives
            variableTable(i) = disjointSet.getRepresentative(variableTable(i))
        }
        val variableList = variableTable.toList

        val relations = visitLogicalRelNode(logicalFilter.getInput, variableList, 0)
        val conditions = conditionRexCalls.toList.map(t => {
            val (rexCall, isNeg) = t
            rexCall.getOperator.getName match {
                case "=" if rexCall.getOperands.get(1).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle r_name = 'EUROPE'
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                    EqualToLiteralCondition(operand, literal)
                case "=" if rexCall.getOperands.get(0).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle 'EUROPE' = r_name
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(0).asInstanceOf[RexLiteral])
                    EqualToLiteralCondition(operand, literal)
                case "<>" if rexCall.getOperands.get(1).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle r_name <> 'EUROPE'
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                    NotEqualToLiteralCondition(operand, literal)
                case "<>" if rexCall.getOperands.get(0).isInstanceOf[RexLiteral] =>
                    assert(!isNeg)
                    // handle 'EUROPE' <> r_name
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    val literal = convertRexLiteralToExpression(rexCall.getOperands.get(0).asInstanceOf[RexLiteral])
                    NotEqualToLiteralCondition(operand, literal)
                case "<" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    LessThanCondition(leftOperand, rightOperand)
                case "<=" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    LessThanOrEqualToCondition(leftOperand, rightOperand)
                case ">" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    GreaterThanCondition(leftOperand, rightOperand)
                case ">=" =>
                    assert(!isNeg)
                    val leftOperand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val rightOperand = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                    GreaterThanOrEqualToCondition(leftOperand, rightOperand)
                case "LIKE" =>
                    assert(rexCall.getOperands.get(0).isInstanceOf[RexInputRef])
                    assert(rexCall.getOperands.get(1).isInstanceOf[RexLiteral])
                    val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val s = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral]).asInstanceOf[StringLiteralExpression]
                    LikeCondition(operand, s, isNeg)
                case "OR" =>
                    convertOrRexCallToCondition(rexCall, variableList, isNeg)
            }
        })

        val computations = mutable.HashMap.empty[Variable, Expression]  // additional computations(in LogicalProject)
        val context = new Context
        variableList.foreach(v => context.dependingVariables(v) = Set(v))
        if (optLogicalProject.isEmpty) {
            context.outputVariables = variableList
            context.computations = computations.toMap
        } else {
            val outputVariablesBuffer = ListBuffer.empty[Variable]
            optLogicalProject.get.getProjects.toList.foreach({
                case call: RexCall =>
                    val expr = convertRexCallToExpression(call, variableList)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(call.getType.getSqlTypeName))
                    computations(variable) = expr
                    outputVariablesBuffer.append(variable)
                    context.dependingVariables(variable) = expr.getVariables()
                case inputRef: RexInputRef =>
                    val variable = variableList(inputRef.getIndex)
                    outputVariablesBuffer.append(variable)
                case literal: RexLiteral =>
                    val expr = convertRexLiteralToExpression(literal)
                    val variable = variableManager.getNewVariable(DataType.fromSqlType(literal.getType.getSqlTypeName))
                    computations(variable) = expr
                    outputVariablesBuffer.append(variable)
                    context.dependingVariables(variable) = Set()
            })
            context.outputVariables = outputVariablesBuffer.toList
            context.requiredVariables = context.outputVariables.flatMap(v => context.dependingVariables(v)).toSet
            context.computations = computations.toMap
        }

        context.relations = relations
        context.conditions = conditions
        context.isFull = variableList.forall(v => context.requiredVariables.contains(v))
        context
    }

    private def processOperandInCondition(operand: RexNode, variableTable: Array[Variable], disjointSet: DisjointSet[Variable],
                                          isNeg: Boolean, result: ListBuffer[(RexCall, Boolean)]): Unit = {
        val rexCall: RexCall = operand.asInstanceOf[RexCall]
        rexCall.getOperator.getName match {
            case "=" =>
                if ((rexCall.getOperands.get(1).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(0).isInstanceOf[RexLiteral])
                    || (rexCall.getOperands.get(0).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(1).isInstanceOf[RexLiteral])) {
                    // this is a filter condition like r_name = 'EUROPE'
                    result.append((rexCall, isNeg))
                } else {
                    // this is a join condition like R.a = S.b
                    val variableList = variableTable.toList
                    val left: Expression = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                    val right: Expression = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)

                    val leftVariable = left.asInstanceOf[SingleVariableExpression].variable
                    val rightVariable = right.asInstanceOf[SingleVariableExpression].variable
                    disjointSet.merge(leftVariable, rightVariable)
                }
            case "<" | "<=" | ">" | ">=" | "LIKE" | "OR" =>
                result.append((rexCall, isNeg))
            case "NOT" =>
                processOperandInCondition(rexCall.getOperands.get(0), variableTable, disjointSet, !isNeg, result)
            case "<>" =>
                assert((rexCall.getOperands.get(1).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(0).isInstanceOf[RexLiteral])
                    || (rexCall.getOperands.get(0).isInstanceOf[RexLiteral] && !rexCall.getOperands.get(1).isInstanceOf[RexLiteral]))
                result.append((rexCall, isNeg))
            case _ =>
                throw new UnsupportedOperationException(s"unsupported operator ${rexCall.getOperator.getName}")
        }
    }

    private def visitLogicalRelNode(relNode: RelNode, variableList: List[Variable], offset: Int): List[Relation] = relNode match {
        case join: LogicalJoin => visitLogicalJoin(join, variableList, offset)
        case aggregate: LogicalAggregate => visitLogicalAggregate(aggregate, variableList, offset)
        case scan: LogicalTableScan => visitLogicalTableScan(scan, variableList, offset)
        // TODO: support sub-queries like '(SELECT COUNT(*) AS cnt, src FROM path GROUP BY src) AS C2'
        case _ => throw new UnsupportedOperationException
    }

    private def visitLogicalJoin(logicalJoin: LogicalJoin, variableList: List[Variable], offset: Int): List[Relation] = {
        val leftFieldCount = logicalJoin.getLeft.getRowType.getFieldCount
        visitLogicalRelNode(logicalJoin.getLeft, variableList, offset) ++
            visitLogicalRelNode(logicalJoin.getRight, variableList, offset + leftFieldCount)
    }

    private def visitLogicalAggregate(logicalAggregate: LogicalAggregate, variableList: List[Variable], offset: Int): List[Relation] = {
        // currently, only support patterns like '(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1'
        assert(logicalAggregate.getGroupCount == 1)
        assert(logicalAggregate.getAggCallList.size() == 1)

        val group = logicalAggregate.getGroupSet.asList().map(_.toInt)
        val variables = variableList.slice(offset, offset + logicalAggregate.getRowType.getFieldCount)
        val func = logicalAggregate.getAggCallList.get(0).getAggregation.toString

        // TODO: support cases that can not be deal with AggregateProjectMergeRule
        assert(logicalAggregate.getInput.isInstanceOf[LogicalTableScan])
        val logicalTableScan = logicalAggregate.getInput.asInstanceOf[LogicalTableScan]
        val tableName = logicalTableScan.getTable.getQualifiedName.head

        if (logicalAggregate.getHints.nonEmpty) {
            List(new AggregatedRelation(tableName, variables, group.toList, func, logicalAggregate.getHints.get(0).kvOptions.get("alias")))
        } else {
            List(new AggregatedRelation(tableName, variables, group.toList, func, s"${func}_${group.mkString("_")}_$tableName"))
        }
    }

    private def visitLogicalTableScan(logicalTableScan: LogicalTableScan, variableList: List[Variable], offset: Int): List[Relation] = {
        val tableName = logicalTableScan.getTable.getQualifiedName.head
        val variables = variableList.slice(offset, offset + logicalTableScan.getRowType.getFieldCount)

        val table = catalogManager.getSchema.getTable(tableName, false).getTable.asInstanceOf[SqlPlusTable]
        // get index of primary keys and map the keys to variables
        val primaryKeys = table.getPrimaryKeys.map(k => table.getTableColumnNames.indexOf(k)).map(variables).toSet
        val cardinality = table.getTableProperties.getOrElse("cardinality", "0").toLong

        if (logicalTableScan.getHints.nonEmpty) {
            List(new TableScanRelation(tableName, variables, logicalTableScan.getHints.get(0).kvOptions.get("alias"), primaryKeys, cardinality))
        } else {
            List(new TableScanRelation(tableName, variables, tableName, primaryKeys, cardinality))
        }
    }

    /**
     * Get the shortest path that connects two relations with variable from and to.
     * Note that such a shortest path can be proofed to be unique.
     *
     * @param nodes all the nodes(relations) in the join tree
     * @param joinTree collection of JoinTreeEdges
     * @param left Expression that lies on the left side of the comparison
     * @param right Expression that lies on the right side of the comparison
     * @return a list of JoinTreeEdge that form the shortest path
     */
    def getShortestInRelationalHyperGraph(nodes: Set[Relation], joinTree: JoinTree,
                                          left: Expression, right: Expression): List[JoinTreeEdge] = {
        val edges = new mutable.HashMap[Relation, mutable.HashSet[Relation]]()

        // add edges in join tree with 2 directions since we may need to go from parent to children
        joinTree.getEdges().foreach(edge => {
            edges.getOrElseUpdate(edge.getSrc, mutable.HashSet.empty).add(edge.getDst)
            edges.getOrElseUpdate(edge.getDst, mutable.HashSet.empty).add(edge.getSrc)
        })

        // we assume that the left or the right variables can be found in a single relation
        // after a transformation proceeding to this method
        val leftVariables = left.getVariables()
        val rightVariables = right.getVariables()

        // collection of nodes that contains the from/to variable, the nodes are of type RelationalHyperEdge
        val fromNodes = nodes.filter(e => e.getNodes().containsAll(leftVariables))
        val toNodes = nodes.filter(e => e.getNodes().containsAll(rightVariables))
        assert(fromNodes.nonEmpty)
        assert(toNodes.nonEmpty)

        // if some relations contain both the fromVariable and toVariable(it is a self comparison)
        // just return a singleton list with a special JoinTreeEdge that has the same element at src and dst
        val intersect = fromNodes.intersect(toNodes)
        if (intersect.nonEmpty) {
            val srcAndDst = intersect.head
            return List(new JoinTreeEdge(srcAndDst, srcAndDst))
        }

        // store the predecessor that on the path from source to the node.
        val nodeToPredecessorDict = new mutable.HashMap[Relation, Relation]()

        val currentQueue = mutable.Queue.empty[Relation]
        val nextQueue = mutable.Queue.empty[Relation]

        fromNodes.foreach(fromNode => {
            currentQueue.enqueue(fromNode)
            nodeToPredecessorDict(fromNode) = null
        })

        val reachedNodes = new mutable.HashSet[Relation]()
        while (reachedNodes.isEmpty) {
            while (currentQueue.nonEmpty) {
                val head = currentQueue.dequeue()
                val connectedNodes = edges(head)

                for (connectedNode <- connectedNodes) {
                    if (nodeToPredecessorDict.contains(connectedNode)) {
                        // we have already visited this node
                    } else {
                        // we have not visited this node yet
                        nodeToPredecessorDict(connectedNode) = head
                        nextQueue.enqueue(connectedNode)
                    }

                    // if we reach some target node, append it to the reachedNodes
                    // then we can recreate the whole path by tracing back
                    if (toNodes.contains(connectedNode)) {
                        reachedNodes.add(connectedNode)
                    }
                }
            }

            // move to next level
            while (nextQueue.nonEmpty)
                currentQueue.enqueue(nextQueue.dequeue())
        }

        // return the path starting from node until the predecessor is null
        def trace(node: Relation): List[JoinTreeEdge] = {
            val predecessor = nodeToPredecessorDict(node)
            if (predecessor == null) {
                // we reach the starting node, no more JoinTreeEdges are needed
                List()
            } else {
                // for each predecessor, trace its paths and append the JoinTreeEdge(p -> node) to all the paths
                joinTree.getEdgeByNodes(predecessor, node) :: trace(predecessor)
            }
        }

        assert(reachedNodes.size == 1)

        // recreate the paths by tracing back
        trace(reachedNodes.head).reverse
    }

    def convertRexNodeToExpression(rexNode: RexNode, variableList: List[Variable]): Expression = {
        rexNode match {
            case rexInputRef: RexInputRef =>
                SingleVariableExpression(variableList(rexInputRef.getIndex))
            case rexCall: RexCall if rexCall.op.getName == "CAST" =>
                convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
            case rexCall: RexCall =>
                convertRexCallToExpression(rexCall, variableList)
            case rexLiteral: RexLiteral =>
                convertRexLiteralToExpression(rexLiteral)
            case _ =>
                throw new UnsupportedOperationException(s"unknown rexNode type ${rexNode.getType}")
        }
    }

    def convertRexCallToExpression(rexCall: RexCall, variableList: List[Variable]): Expression = {
        rexCall.op.getName match {
            case "+" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                if (left.getType() == TimestampDataType && right.getType() == IntervalDataType) {
                    TimestampPlusIntervalExpression(left, right)
                } else if (left.getType() == DateDataType && right.getType() == IntervalDataType) {
                    DatePlusIntervalExpression(left, right)
                } else {
                    DataTypeCasting.promote(left.getType(), right.getType()) match {
                        case DoubleDataType => DoublePlusDoubleExpression(left, right)
                        case LongDataType => LongPlusLongExpression(left, right)
                        case IntDataType => IntPlusIntExpression(left, right)
                    }
                }
            case "-" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                if (left.getType() == TimestampDataType && right.getType() == IntervalDataType) {
                    TimestampMinusIntervalExpression(left, right)
                } else if (left.getType() == DateDataType && right.getType() == IntervalDataType) {
                    DateMinusIntervalExpression(left, right)
                } else {
                    DataTypeCasting.promote(left.getType(), right.getType()) match {
                        case DoubleDataType => DoubleMinusDoubleExpression(left, right)
                        case LongDataType => LongMinusLongExpression(left, right)
                        case IntDataType => IntMinusIntExpression(left, right)
                    }
                }
            case "*" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                DataTypeCasting.promote(left.getType(), right.getType()) match {
                    case DoubleDataType => DoubleTimesDoubleExpression(left, right)
                    case LongDataType => LongTimesLongExpression(left, right)
                    case IntDataType => IntTimesIntExpression(left, right)
                }
            case "/" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val right = convertRexNodeToExpression(rexCall.getOperands.get(1), variableList)
                DataTypeCasting.promote(left.getType(), right.getType()) match {
                    case DoubleDataType => DoubleDivideByDoubleExpression(left, right)
                    case LongDataType => LongDivideByLongExpression(left, right)
                    case IntDataType => IntDivideByIntExpression(left, right)
                }
            case "CASE" =>
                buildCaseWhenExpression(rexCall.getOperands.toList, variableList)
            case "EXTRACT" =>
                buildExtractExpression(rexCall.getOperands.toList, variableList)
            case _ =>
                throw new UnsupportedOperationException("unsupported op " + rexCall.op.getName)
        }
    }

    def convertRexCallToOperatorAndExpressions(rexCall: RexCall, variableList: List[Variable]): (Operator, List[Expression]) = {
        rexCall.getOperator.getName match {
            case "SEARCH" =>
                val left = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList).asInstanceOf[SingleVariableExpression]
                val dataType = left.getType()
                val sarg = rexCall.getOperands.get(1).asInstanceOf[RexLiteral].getValue.asInstanceOf[Sarg[_]]
                val ranges = sarg.rangeSet.asRanges().toList
                val isNeg = !ranges.get(0).hasLowerBound || ranges.get(0).lowerBoundType() == BoundType.OPEN

                dataType match {
                    case StringDataType =>
                        val hashSet = mutable.HashSet.empty[String]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[NlsString].getValue)
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[NlsString].getValue)
                        })
                        (StringInLiterals(hashSet.toList, isNeg), List(left))
                    case IntDataType =>
                        val hashSet = mutable.HashSet.empty[Int]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].intValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].intValue())
                        })
                        (IntInLiterals(hashSet.toList, isNeg), List(left))
                    case LongDataType =>
                        val hashSet = mutable.HashSet.empty[Long]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].longValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].longValue())
                        })
                        (LongInLiterals(hashSet.toList, isNeg), List(left))
                    case DoubleDataType =>
                        val hashSet = mutable.HashSet.empty[Double]
                        ranges.foreach(r => {
                            if (r.hasLowerBound)
                                hashSet.add(r.lowerEndpoint().asInstanceOf[java.math.BigDecimal].doubleValue())
                            if (r.hasUpperBound)
                                hashSet.add(r.upperEndpoint().asInstanceOf[java.math.BigDecimal].doubleValue())
                        })
                        (DoubleInLiterals(hashSet.toList, isNeg), List(left))
                }
            case "=" =>
                val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val literal = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                literal match {
                    case StringLiteralExpression(lit) => (StringEqualToLiteral(lit, false), List(operand))
                    case IntLiteralExpression(lit) => (IntEqualToLiteral(lit, false), List(operand))
                    case LongLiteralExpression(lit) => (LongEqualToLiteral(lit, false), List(operand))
                    case DoubleLiteralExpression(lit) => (DoubleEqualToLiteral(lit, false), List(operand))
                }
            case "LIKE" =>
                val operand = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val s = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral]).asInstanceOf[StringLiteralExpression]
                (StringMatch(s.lit, false), List(operand))
            case _ =>
                throw new UnsupportedOperationException()
        }
    }

    def convertRexLiteralToExpression(rexLiteral: RexLiteral): LiteralExpression = {
        rexLiteral.getType.getSqlTypeName.getName match {
            case "VARCHAR" => StringLiteralExpression(rexLiteral.getValue.asInstanceOf[NlsString].getValue)
            case "INTEGER" => IntLiteralExpression(rexLiteral.getValue.toString.toInt)
            case "BIGINT" => LongLiteralExpression(rexLiteral.getValue.toString.toLong)
            case "DECIMAL" => DoubleLiteralExpression(rexLiteral.getValue.toString.toDouble)
            case "CHAR" => StringLiteralExpression(rexLiteral.getValue.asInstanceOf[NlsString].getValue)
            case "INTERVAL_DAY" => IntervalLiteralExpression(rexLiteral.getValue.toString.toLong)
            case "DATE" => DateLiteralExpression(rexLiteral.getValue.asInstanceOf[GregorianCalendar].getTime.getTime)
            case _ => throw new UnsupportedOperationException("unsupported literal type " + rexLiteral.getType.getSqlTypeName.getName)
        }
    }

    def buildCaseWhenExpression(operands: List[RexNode], variableList: List[Variable]): CaseWhenExpression = {
        var index = 0
        assert(operands.size % 2 == 1)  // we should have a default branch
        val buffer = ListBuffer.empty[(Operator, List[Expression], Expression)]

        while (index + 1 < operands.size) {
            // when clause
            val w = operands(index)
            val (wOp, wExprs) = convertRexCallToOperatorAndExpressions(w.asInstanceOf[RexCall], variableList)
            // then clause
            val t = operands(index + 1)
            val tExpr = convertRexNodeToExpression(t, variableList)
            buffer.append((wOp, wExprs, tExpr))
            index += 2
        }

        // now we reach the default branch
        val dExpr = convertRexNodeToExpression(operands.last, variableList)
        CaseWhenExpression(buffer.toList, dExpr)
    }

    def buildExtractExpression(operands: List[RexNode], variableList: List[Variable]): Expression = {
        val flag = operands.get(0).asInstanceOf[RexLiteral].getValue.toString
        flag match {
            case "YEAR" =>
                val from = convertRexNodeToExpression(operands(1), variableList)
                ExtractYearExpression(from)
            case _ => throw new UnsupportedOperationException("unsupported flag " + flag + " in EXTRACT.")
        }
    }

    private def convertOrRexCallToCondition(rexCall: RexCall, variableList: List[Variable], isNeg: Boolean): Condition = {
        if (rexCall.getOperands.forall(n => n.isInstanceOf[RexCall] && n.asInstanceOf[RexCall].getOperator.getName == "=")) {
            val leftOperands = rexCall.getOperands.map(n => n.asInstanceOf[RexCall].getOperands.get(0))
            val rightOperands = rexCall.getOperands.map(n => n.asInstanceOf[RexCall].getOperands.get(1))
            assert(leftOperands.forall(n => n.isInstanceOf[RexInputRef]) && rightOperands.forall(n => n.isInstanceOf[RexLiteral]))

            val headExpr = convertRexNodeToExpression(leftOperands.head, variableList)
            val leftExprs = leftOperands.map(n => convertRexNodeToExpression(n, variableList))
            val literals = rightOperands.map(n => convertRexLiteralToExpression(n.asInstanceOf[RexLiteral])).toList
            if (leftExprs.forall(expr => expr == headExpr)) {
                InCondition(headExpr, literals, isNeg)
            } else {
                assert(!isNeg)
                val conditions = leftExprs.zip(literals).map(t => EqualToLiteralCondition(t._1, t._2)).toList
                OrCondition(conditions, leftExprs.toSet)
            }
        } else {
            assert(!isNeg)
            val rexCalls = rexCall.getOperands.map(n => n.asInstanceOf[RexCall]).toList
            val converted = rexCalls.map(c => convertRexCallToLitCondition(c, variableList, false))
            OrCondition(converted.map(t => t._1), converted.flatMap(t => t._2).toSet)
        }
    }

    private def convertRexCallToLitCondition(rexCall: RexCall, variableList: List[Variable], isNeg: Boolean): (Condition, Set[Expression]) = {
        rexCall.getOperator.getName match {
            case "AND" =>
                val rexCalls = rexCall.getOperands.map(n => n.asInstanceOf[RexCall])
                val converted = rexCalls.map(c => convertRexCallToLitCondition(c, variableList, isNeg)).toList
                val involved = converted.flatMap(t => t._2).toSet
                (AndCondition(converted.map(t => t._1), involved), involved)
            case "OR" =>
                val rexCalls = rexCall.getOperands.map(n => n.asInstanceOf[RexCall])
                val converted = rexCalls.map(c => convertRexCallToLitCondition(c, variableList, isNeg)).toList
                val involved = converted.flatMap(t => t._2).toSet
                (OrCondition(converted.map(t => t._1), involved), involved)
            case "=" =>
                val expr = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val lit = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                (EqualToLiteralCondition(expr, lit), Set(expr))
            case "<>" =>
                val expr = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val lit = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral])
                (NotEqualToLiteralCondition(expr, lit), Set(expr))
            case "LIKE" =>
                val expr = convertRexNodeToExpression(rexCall.getOperands.get(0), variableList)
                val s = convertRexLiteralToExpression(rexCall.getOperands.get(1).asInstanceOf[RexLiteral]).asInstanceOf[StringLiteralExpression]
                (LikeCondition(expr, s, isNeg), Set(expr))
            case "NOT" =>
                convertRexCallToLitCondition(rexCall.getOperands.get(0).asInstanceOf[RexCall], variableList, !isNeg)
        }
    }
}
