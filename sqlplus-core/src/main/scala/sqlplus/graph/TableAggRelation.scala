package sqlplus.graph

import sqlplus.expression.Variable

class TableAggRelation(val tableName: String, var variables: List[Variable], val tableDisplayName: String, var aggRelation: List[AggregatedRelation], val primaryKeys: Set[Variable], val cardinality: Long) extends Relation {
  def getTableName(): String = tableName

  def getAggRelation(): List[AggregatedRelation] = aggRelation

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

  override def toString: String = {
    val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
    s"TableAggRelation;id=${getRelationId()};source=$tableName;cols=$columns;tableDisplayName=$tableDisplayName;AggList=$aggRelation"
  }

  override def getTableDisplayName(): String = tableDisplayName

  override def replaceVariables(map: Map[Variable, Variable]): Relation = {
    val newVariables = variables.map(v => if (map.contains(v)) map(v) else v)
    new TableAggRelation(tableName, newVariables, tableDisplayName, aggRelation, primaryKeys, cardinality)
  }

  override def getPrimaryKeys(): Set[Variable] = primaryKeys

  override def getCardinality(): Long = cardinality

  // Must call
  initVariableList()
}

