package sqlplus.convert

import sqlplus.graph.{AggregatedRelation, Relation, RelationalHyperGraph, TableAggRelation, TableScanRelation}
import sqlplus.gyo.GyoAlgorithm

import scala.collection.mutable.ListBuffer

class DryRunHandler(gyo: GyoAlgorithm) {
    def handle(context: Context): Option[HandleResult] = {
        val relations = context.removeAggRelation(context.relations)
        context.relations = relations
        val requiredVariables = context.requiredVariables
        val groupByVariables = context.groupByVariables
        val aggregations = context.aggregations
        val topVariables = if (aggregations.nonEmpty && groupByVariables.nonEmpty) groupByVariables.toSet else requiredVariables
        val relationalHyperGraph = relations.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r))

        gyo.dryRun(relationalHyperGraph, topVariables).map(HandleResult.fromGyoResult)
    }
}
