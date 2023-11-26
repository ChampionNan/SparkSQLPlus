package sqlplus.graph

import sqlplus.expression.Variable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TableScanRelation(val tableName: String, val variables: List[Variable], val tableDisplayName: String) extends Relation {
    def getTableName(): String = tableName

    var aggList: List[AggregatedRelation] = List()

    def addAgg(agg: AggregatedRelation) = {
        aggList = aggList :+ agg
    }

    def getAggList: List[AggregatedRelation] = aggList

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"TableScanRelation;id=${getRelationId()};source=$tableName;cols=$columns;tableDisplayName=$tableDisplayName"
    }

    override def getTableDisplayName(): String = tableDisplayName
}
