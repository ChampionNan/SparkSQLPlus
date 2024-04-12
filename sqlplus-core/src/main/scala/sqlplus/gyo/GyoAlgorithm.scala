package sqlplus.gyo

import sqlplus.expression.Variable
import sqlplus.graph._
import sqlplus.gyo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This algorithm is modeled as a BFS search of valid states. A state contains a forest and the remaining hyperGraph.
 */
class GyoAlgorithm {
    def run(hyperGraph: RelationalHyperGraph, outputVariables: Set[Variable], pruneEnable: Boolean): GyoResult = {
        runAlgorithm(hyperGraph, outputVariables, None, pruneEnable)
    }

    def runWithFixRoot(hyperGraph: RelationalHyperGraph, fixRootRelation: Relation, pruneEnable: Boolean): GyoResult = {
        // run the algorithm with outputVariables = all variables
        runAlgorithm(hyperGraph, hyperGraph.getEdges().flatMap(r => r.getNodes()), Some(fixRootRelation), pruneEnable)
    }

    private def runAlgorithm(hyperGraph: RelationalHyperGraph, outputVariables: Set[Variable], optFixRootRelation: Option[Relation],
                             pruneEnable: Boolean): GyoResult = {
        val initState = new GyoState(hyperGraph, Forest.create(hyperGraph))
        var isFreeConnex = true

        // attach the tree represented by $child to the tree represented by $parent
        // $child will be removed in hyperGraph
        def attach(parent: Relation, child: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = forest.getTree(parent)
            val yTreeNode = forest.getTree(child)

            // reduce the unnecessary AuxiliaryRelations here
            if (child.isInstanceOf[AuxiliaryRelation] && child.getNodes().subsetOf(parent.getNodes())) {
                // there is no need to attach an AuxiliaryRelation to a parent that contains all its variables
                // e.g., R(A,B,C) -> [B,C] -> S(B,C,D) = R(A,B,C) -> S(B,C,D)
                assert(yTreeNode.getChildren().nonEmpty)
                val newTreeNode = xTreeNode.attachAll(yTreeNode.getChildren())
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            } else {
                // general cases
                val newTreeNode = xTreeNode.attach(yTreeNode)
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            }
        }

        // for hyperGraph: replace $originalRelation with $newRelation
        // for forest: create a new tree node for $newRelation and attach the original tree node to it
        def replace(newRelation: Relation, originalRelation: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = gyo.LeafTreeNode(newRelation)
            val yTreeNode = forest.getTree(originalRelation)
            val newTreeNode = xTreeNode.attach(yTreeNode)
            val newForest = forest.removeTree(yTreeNode).addTree(newTreeNode)
            val newGraph = graph.removeHyperEdge(originalRelation).addHyperEdge(newRelation)
            new GyoState(newGraph, newForest)
        }

        // compute the reachable valid states in phase 1
        def phase1NextStates(gyoState: GyoState): Option[List[GyoState]] = {
            // reduce relations without output variables
            val remainingRelations = gyoState.getRelationalHyperGraph.getEdges()
            val nonOutputRelations = remainingRelations.filter(r => r.getNodes().intersect(outputVariables).isEmpty)

            val reductions = ListBuffer.empty[(Relation, Relation)]
            val removes = ListBuffer.empty[(Relation, Set[Variable])]
            for {
                x <- nonOutputRelations
                y <- remainingRelations
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes())) {
                    reductions.append((x, y))       // reduce x to y
                } else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remainingRelations.forall(r => r == x || !r.getNodes().contains(v)))) {
                        // variables in diff belong to x only
                        // create AuxiliaryRelation with variables x.getNodes() - diff
                        removes.append((x, diff))
                    }
                }
            }

            if (reductions.isEmpty && removes.isEmpty) {
                // try to find some non-output variables that belongs to one relation
                val outputRelations = remainingRelations.filter(r => r.getNodes().intersect(outputVariables).nonEmpty)
                outputRelations.foreach(r => {
                    val variables = r.getNodes()
                    // nonOutput variables that belongs to r only
                    val removable = variables.filter(v => !outputVariables.contains(v) && remainingRelations.forall(p => p == r || !p.getNodes().contains(v)))
                    if (removable.nonEmpty) {
                        removes.append((r, removable))
                    }
                })
            }

            if (reductions.nonEmpty) {
                // perform reduction first if we have relations like R(A,B,C) and S(B,C)
                Some(reductions.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)))
            } else if (removes.nonEmpty) {
                Some(removes.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }))
            } else {
                None
            }
        }

        def phase2NextStates(stateWithSubsetAndOptRoot: (GyoState, Set[Relation], Option[Relation])): Option[List[(GyoState, Set[Relation], Option[Relation])]] = {
            val gyoState = stateWithSubsetAndOptRoot._1
            val subset = stateWithSubsetAndOptRoot._2
            val optRoot = stateWithSubsetAndOptRoot._3

            val remaining = gyoState.getRelationalHyperGraph.getEdges()
            val reductions = ListBuffer.empty[(Relation, Relation)]
            val removes = ListBuffer.empty[(Relation, Set[Variable])]

            for {
                x <- remaining
                y <- remaining
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                // if we have picked the root relation and x matches with the root relation,
                // we need to skip in order to keep the root relation till the end
                val optNeedSkip = optRoot.map(r => {
                    x match {
                        case aux: AuxiliaryRelation => r == aux.supportingRelation
                        case _ => r == x
                    }
                })

                // if optRoot is unset or we don't need to skip
                if (optNeedSkip.isEmpty || !optNeedSkip.get) {
                    if (x.getNodes().subsetOf(y.getNodes()))
                        reductions.append((x, y))
                    else {
                        val diff = x.getNodes() -- y.getNodes()
                        if (diff.forall(v => remaining.forall(r => r == x || !r.getNodes().contains(v)))) {
                            removes.append((x, diff))
                        }
                    }
                }
            }

            if (reductions.nonEmpty) {
                Some(reductions.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)).map(s => (s, subset, optRoot)))
            } else if (removes.nonEmpty) {
                Some(removes.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }).map(s => (s, subset, optRoot)))
            } else {
                None
            }
        }

        // phase 1
        val seenStates = mutable.HashSet.empty[GyoState]
        val queue1 = mutable.Queue.empty[GyoState]
        val stableStates = mutable.HashSet.empty[GyoState]
        queue1.enqueue(initState)
        while (queue1.nonEmpty) {
            val head = queue1.dequeue()
            val next = phase1NextStates(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenStates.contains(p)) {
                        seenStates.add(p)
                        queue1.enqueue(p)
                    }
                })
            } else {
                // a state is stable if we cannot find any next state from it
                stableStates.add(head)
            }
        }

        assert(stableStates.nonEmpty)

        if (outputVariables.nonEmpty &&
            stableStates.exists(s => s.getRelationalHyperGraph.getEdges().exists(r => r.getNodes().exists(v => !outputVariables.contains(v))))) {
            isFreeConnex = false
        }

        // phase 2
        val finalStates = mutable.HashSet.empty[(GyoState, Set[Relation], Option[Relation])]
        val queue2 = mutable.Queue.empty[(GyoState, Set[Relation], Option[Relation])]
        seenStates.clear()
        // search final states from stable states
        stableStates.foreach(state => {
            val optRoot = if (optFixRootRelation.nonEmpty) {
                optFixRootRelation
            } else {
                // if prune is enabled, pick the plans with smallest relation at the root
                val subsetRelations = state.getRelationalHyperGraph.getEdges()
                val subsetRelationsWithCardinality = subsetRelations.map {
                    case r@(relation: AuxiliaryRelation) => (r.supportingRelation, relation.supportingRelation.getCardinality())
                    case r => (r, r.getCardinality())
                }
                if (pruneEnable && subsetRelationsWithCardinality.forall(t => t._2 > 0))
                    Some(subsetRelationsWithCardinality.minBy(t => t._2)._1) else None
            }

            queue2.enqueue((state, state.getRelationalHyperGraph.getEdges(), optRoot))
        })
        while (queue2.nonEmpty) {
            val head = queue2.dequeue()
            val next = phase2NextStates(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenStates.contains(p._1)) {
                        seenStates.add(p._1)
                        queue2.enqueue(p)
                    }
                })
            } else {
                finalStates.add(head)
            }
        }

        // in final state, there should be only one relation in hyperGraph
        if (finalStates.map(t => t._1).forall(fs => fs.getRelationalHyperGraph.getEdges().size == 1)) {
            val rootsAndSubset = finalStates.map(s => (s._1.getForest.getTree(s._1.getRelationalHyperGraph.getEdges().head), s._2)).toList
            val candidates = rootsAndSubset.map(ras => {
                val (root, edges, hyperGraph) = convertToJoinTreeWithHyperGraph(ras._1)
                val joinTree = JoinTree(root, edges, if (outputVariables.nonEmpty) ras._2.intersect(hyperGraph.getEdges()) else Set(), optFixRootRelation.nonEmpty)
                (joinTree, hyperGraph)
            })

            val finalCandidates = if (outputVariables.nonEmpty) candidates.filter(c => c._1.root.getNodes().intersect(outputVariables).nonEmpty) else candidates
            val filteredCandidates = if (pruneEnable) finalCandidates.filter(t => t._1.getEdges().count(e => e.getSrc == t._1.root || e.getDst == t._1.root) < 4) else finalCandidates
            GyoResult(filteredCandidates, isFreeConnex, List.empty)
        } else {
            throw new IllegalStateException("some final states have more than 1 relation")
        }
    }

    // traverse the tree nodes from the root, construct a JoinTree and RelationalHyperGraph
    private def convertToJoinTreeWithHyperGraph(root: TreeNode): (Relation, Set[JoinTreeEdge], RelationalHyperGraph) = {
        def fun(node: TreeNode, parent: Relation): (Set[JoinTreeEdge], Set[Relation]) = {
            node match {
                case InternalTreeNode(relation, children) =>
                    val childrenResult = children.map(n => fun(n, relation))
                        .foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, r) => (z._1 ++ r._1, z._2 ++ r._2))
                    (childrenResult._1 + new JoinTreeEdge(parent, relation), childrenResult._2 + relation)
                case LeafTreeNode(relation) =>
                    (Set(new JoinTreeEdge(parent, relation)), Set(relation))
            }
        }

        root match {
            case LeafTreeNode(relation) =>
                (relation, Set(), RelationalHyperGraph.EMPTY.addHyperEdge(relation))
            case InternalTreeNode(relation, children) =>
                val childrenResult = children.map(n => fun(n, relation))
                    .foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, r) => (z._1 ++ r._1, z._2 ++ r._2))
                val graph = childrenResult._2.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r)).addHyperEdge(relation)
                (relation, childrenResult._1, graph)
        }
    }

    def isAcyclic(hyperGraph: RelationalHyperGraph): Boolean = {
        def nextGraph(inputHyperGraph: RelationalHyperGraph): Option[RelationalHyperGraph] = {
            val remaining = inputHyperGraph.getEdges()
            val reductions = ListBuffer.empty[(Relation, Relation)]
            val removes = ListBuffer.empty[(Relation, Set[Variable])]

            for {
                x <- remaining
                y <- remaining
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes()))
                    reductions.append((x, y))
                else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remaining.forall(r => r == x || !r.getNodes().contains(v)))) {
                        removes.append((x, diff))
                    }
                }
            }

            if (reductions.nonEmpty) {
                Some(inputHyperGraph.removeHyperEdge(reductions.head._1))
            } else if (removes.nonEmpty) {
                val (relation, variables) = removes.head
                Some(inputHyperGraph.removeHyperEdge(relation).addHyperEdge(relation.removeVariables(variables)))
            } else {
                None
            }
        }

        var currentHyperGraph = hyperGraph
        var optNextGraph = nextGraph(currentHyperGraph)
        while (optNextGraph.nonEmpty) {
            currentHyperGraph = optNextGraph.get
            optNextGraph = nextGraph(currentHyperGraph)
        }

        // acyclic if we can reduce the input hyperGraph until a single relation is left
        currentHyperGraph.getEdges().size == 1
    }
}
