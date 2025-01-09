package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.AggregatedRelation;
import sqlplus.graph.TableAggRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableAggJoinTreeNode extends JoinTreeNode {
    String source;
    List<String> columns;

    List<Integer> aggList;

    public TableAggJoinTreeNode(TableAggRelation relation, List<String> reserve, List<Integer> hintJoinOrder) {
        super(relation.getRelationId(), "TableAggRelation", relation.getTableDisplayName(), reserve, hintJoinOrder);
        this.source = relation.getTableName();
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
        List<AggregatedRelation> aggRelations = JavaConverters.seqAsJavaList(relation.getAggRelation());
        this.aggList = new ArrayList<>();
        for (AggregatedRelation c : aggRelations) {
            aggList.add(c.getRelationId());
        }
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<Integer> getAggList() { return aggList; }

    public void setAggList(List<Integer> aggList) { this.aggList = aggList; }
}
