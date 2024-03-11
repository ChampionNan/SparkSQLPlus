package sqlplus.graph

import sqlplus.expression.Variable
import scala.collection.mutable.ListBuffer

class TableAggRelation(val tableName: String, var variables: List[Variable], val tableDisplayName: String, var aggRelation: List[AggregatedRelation], val primaryKeys: Set[Variable]) extends Relation {
  def getTableName(): String = tableName

  def getAggRelation(): List[AggregatedRelation] = aggRelation

  def getAggRelationId(): List[Int] = {
    var aggRelationId: ListBuffer[Int] = ListBuffer()
    aggRelation.foreach(r => {
      val a = r.getRelationId()
      aggRelationId += a
    })
    aggRelationId.toList
  }

  // Initialization
  def initVariableList(): List[Variable] = {
    val aggVars = aggRelation.foldLeft(List[Variable]())((x, y) => x ::: y.getVariableList())
    variables = variables.union(aggVars).distinct
    variables
  }

  def updateVariableList(newAgg: AggregatedRelation): List[Variable] = {
    variables = variables.union(newAgg.getVariableList()).distinct
    variables
  }

  def addAggRelation(newAgg: AggregatedRelation): List[AggregatedRelation] = {
    updateVariableList(newAgg)
    aggRelation = aggRelation :+ newAgg
    aggRelation
  }

  override def getVariableList(): List[Variable] = variables

  def concatInside: String = {
    var res = ""
    getAggRelation().foreach(rel => {
      res += rel.toString
    })
    res
  }

  def getAggId: String = {
    getAggRelation().map(r => r.getRelationId()).mkString(",")
  }

  override def toString: String = {
    val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
    val aggId = getAggId
    s"TableAggRelation;id=${getRelationId()};source=$tableName;cols=$columns;tableDisplayName=$tableDisplayName;AggList=$aggId\n${concatInside}"
  }

  override def getTableDisplayName(): String = tableDisplayName

  override def getPrimaryKeys(): Set[Variable] = primaryKeys

  override def replaceVariables(map: Map[Variable, Variable]): Relation = {
    val newVariables = variables.map(v => if (map.contains(v)) map(v) else v)
    new TableAggRelation(tableName, newVariables, tableDisplayName, aggRelation, primaryKeys)
  }

  // Must call
  initVariableList()
}

