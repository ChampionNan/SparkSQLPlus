package sqlplus.graph

import sqlplus.expression.Variable
import scala.collection.mutable.ListBuffer

class TableAggRelation(val tableName: String, var variables: List[Variable], val tableDisplayName: String, var aggRelation: List[AggregatedRelation]) extends Relation {
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

  override def toString: String = {
    val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
    s"TableAggRelation;id=${getRelationId()};source=$tableName;cols=$columns;tableDisplayName=$tableDisplayName;AggList=$getAggRelationId\n${concatInside}"
  }

  override def getTableDisplayName(): String = tableDisplayName

  // Must call
  initVariableList()
}

