package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.TableAggRelation;

import java.util.List;
import java.util.stream.Collectors;

public class TableAggJoinTreeNode extends JoinTreeNode {
    String source;
    List<String> columns;

    public TableAggJoinTreeNode(TableAggRelation relation, List<String> reserve, List<Integer> hintJoinOrder) {
        super(relation.getRelationId(), "TableAggRelation", relation.getTableDisplayName(), reserve, hintJoinOrder);
        this.source = relation.getTableName();
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
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
}
