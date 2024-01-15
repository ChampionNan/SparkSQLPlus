package sqlplus.convert

import sqlplus.expression.{Expression, Variable}
import sqlplus.graph.{ComparisonHyperGraph, JoinTree}

case class RunResult(joinTreesWithComparisonHyperGraph: List[(JoinTree, ComparisonHyperGraph)],
                     outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean, isFreeConnex: Boolean,
                     groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                     optTopK: Option[TopK])

object RunResult {
    def buildFromSingleResult(result: (JoinTree, ComparisonHyperGraph),
                              outputVariables: List[Variable], computations: List[(Variable, Expression)], isFull: Boolean, isFreeConnex: Boolean,
                              groupByVariables: List[Variable], aggregations: List[(Variable, String, List[Expression])],
                              optTopK: Option[TopK]): RunResult = {
        RunResult(List(result), outputVariables, computations, isFull, isFreeConnex, groupByVariables, aggregations, optTopK)
    }
}
