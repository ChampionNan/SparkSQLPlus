package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.BagRelation;
import sqlplus.graph.Relation;

import java.util.List;
import java.util.stream.Collectors;

public class BagJoinTreeNode extends JoinTreeNode {
    List<String> internal;
    List<String> columns;
    String alias;
    String internalRelations;

    public BagJoinTreeNode(BagRelation relation, List<String> reserve, List<Integer> hintJoinOrder) {
        super(relation.getRelationId(), "BagRelation", relation.getTableDisplayName(), reserve, hintJoinOrder);
        this.internal = JavaConverters.seqAsJavaList(relation.getInternalRelations()).stream().map(Relation::getTableDisplayName).collect(Collectors.toList());
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
        this.alias = relation.getTableDisplayName();
        this.internalRelations = relation.getInId() + '\n' + relation.concatInside();
    }

    public List<String> getInternal() {
        return internal;
    }

    public void setInternal(List<String> internal) {
        this.internal = internal;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getInternalRelations() { return internalRelations; }

    public void setInternalRelations(String internalRelations) { this.internalRelations = internalRelations; }

}