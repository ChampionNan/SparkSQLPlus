package sqlplus.convert

import sqlplus.graph.{AggregatedRelation, Relation, RelationalHyperGraph, TableAggRelation, TableScanRelation}
import sqlplus.gyo.GyoAlgorithm

import scala.collection.mutable.ListBuffer

class DryRunHandler(gyo: GyoAlgorithm) {
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
    def handle(context: Context): Option[HandleResult] = {
        val relations = removeAggRelation(context.relations)
        // val relations = context.relations
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        gyo.dryRun(relationalHyperGraph, topVariables).map(HandleResult.fromGyoResult)
    }
}
