package sqlplus.graph

import AuxiliaryRelation.createFrom
import sqlplus.expression.Variable

class AuxiliaryRelation(val tableName: String, val variables: List[Variable], val supportingRelation: Relation, val tableDisplayName: String) extends Relation {
    override def getTableName(): String = tableName

    override def getVariableList(): List[Variable] = variables

    override def toString: String = {
        val columns = variables.map(n => n.name + ":" + n.dataType).mkString("(", ",", ")")
        s"AuxiliaryRelation;id=${getRelationId()};source=$tableName;cols=$columns;tableDisplayName=$tableDisplayName;supportingRelation=${supportingRelation.getRelationId}\n${supportingRelation}"
    }

    override def getTableDisplayName(): String = tableDisplayName
}

object AuxiliaryRelation {
    /**
     * create a AuxiliaryRelation from the given relation.
     * @param relation the relation
     * @param remainVariables the remaining variables
     * @return a new AuxiliaryRelation
     */
    def createFrom(supportingRelation: Relation, remainVariables: List[Variable]): AuxiliaryRelation = {
        val name = s"${supportingRelation.getTableName()}"
        val displayName = s"${supportingRelation.getTableDisplayName()}Aux${scala.util.Random.nextInt(100).toString}"
        assert(remainVariables.forall(v => supportingRelation.getNodes().contains(v)))
        new AuxiliaryRelation(name, remainVariables, supportingRelation, displayName)
    }
}
