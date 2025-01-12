package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.AggregatedRelation;
import sqlplus.graph.BagRelation;
import sqlplus.graph.Relation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BagJoinTreeNode extends JoinTreeNode {
    List<String> internal;
    List<String> columns;

    List<Integer> inId;

    public BagJoinTreeNode(BagRelation relation, List<String> reserve, List<Integer> hintJoinOrder, Long cardinality) {
        super(relation.getRelationId(), "BagRelation", relation.getTableDisplayName(), reserve, hintJoinOrder, cardinality);
        this.internal = JavaConverters.seqAsJavaList(relation.getInternalRelations()).stream().map(Relation::getTableDisplayName).collect(Collectors.toList());
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
        List<Relation> inRelations = JavaConverters.seqAsJavaList(relation.getInternalRelations());
        this.inId = new ArrayList<>();
        for (Relation c : inRelations) {
            inId.add(c.getRelationId());
        }
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

    public List<Integer> getInId() { return inId; }

    public void setInId(List<Integer> inId) { this.inId = inId; }
}