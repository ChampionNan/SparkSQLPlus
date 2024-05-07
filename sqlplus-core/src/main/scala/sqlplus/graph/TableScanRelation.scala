package sqlplus.graph

import sqlplus.expression.Variable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TableScanRelation(val tableName: String, val variables: List[Variable], val tableDisplayName: String, val primaryKeys: Set[Variable], val cardinality: Long) extends Relation {
    def getTableName(): String = tableName

    var aggList: List[AggregatedRelation] = List()

    var aggAttachAlready: Boolean = false

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

    override def getPrimaryKeys(): Set[Variable] = primaryKeys

    override def replaceVariables(map: Map[Variable, Variable]): Relation = {
        val newVariables = variables.map(v => if (map.contains(v)) map(v) else v)
        new TableScanRelation(tableName, newVariables, tableDisplayName, primaryKeys, cardinality)
    }

    override def getCardinality(): Long = cardinality
}
