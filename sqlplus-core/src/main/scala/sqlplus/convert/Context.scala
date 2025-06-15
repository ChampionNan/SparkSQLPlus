package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{AggregatedRelation, Relation, TableAggRelation, TableScanRelation}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Context {
    var relations = List.empty[Relation]
    var conditions = List.empty[Condition]
    var outputVariables = List.empty[Variable]
    var requiredVariables = Set.empty[Variable]
    var computations = Map.empty[Variable, Expression]
    var isFull: Boolean = false
    var groupByVariables = List.empty[Variable]
    var aggregations = List.empty[(Variable, String, List[Expression])]
    var optTopK: Option[TopK] = None

    val dependingVariables = mutable.HashMap.empty[Variable, Set[Variable]]

    def removeAggRelation(relations: List[Relation]): List[Relation] = {
        val aggRelations = relations.filter({
            case r: AggregatedRelation => true
            case _ => false
        }).to[ListBuffer]
        // No need to do the modification
        if (aggRelations.isEmpty) {
            relations
        }
        val tableRelations = relations.filter({
            case r: TableScanRelation => true
            case _ => false
        }).to[ListBuffer]
        val otherRelations = relations.filter({
            case r: AggregatedRelation => false
            case s: TableScanRelation => false
            case _ => true
        })
        var newTableAggRelation: ListBuffer[TableAggRelation] = ListBuffer()


        for (table <- tableRelations; agg <- aggRelations; if table.getVariableList().intersect(agg.getVariableList()).nonEmpty && !table.asInstanceOf[TableScanRelation].aggAttachAlready) {
            table.asInstanceOf[TableScanRelation].addAgg(agg.asInstanceOf[AggregatedRelation])
            table.asInstanceOf[TableScanRelation].aggAttachAlready = true
            aggRelations -= agg
        }

        for (table <- tableRelations; agg <- aggRelations; if table.getVariableList().intersect(agg.getVariableList()).nonEmpty) {
            table.asInstanceOf[TableScanRelation].addAgg(agg.asInstanceOf[AggregatedRelation])
            aggRelations -= agg
        }

        for (table <- tableRelations; if table.asInstanceOf[TableScanRelation].getAggList.nonEmpty) {
            var newAggTable = new TableAggRelation(table.getTableName(), table.getVariableList(), table.getTableDisplayName(), table.asInstanceOf[TableScanRelation].getAggList, table.getPrimaryKeys(), table.getCardinality())
            newAggTable.initVariableList()
            newTableAggRelation += newAggTable
            tableRelations -= table
        }

        var retList: List[Relation] = otherRelations ::: newTableAggRelation.toList ::: tableRelations.toList
        retList
    }
}
