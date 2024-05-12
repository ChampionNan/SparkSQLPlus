package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.AuxiliaryRelation;

import java.util.List;
import java.util.stream.Collectors;

public class AuxiliaryJoinTreeNode extends JoinTreeNode {
    int support;
    List<String> columns;
    String alias;
    String source;

    public AuxiliaryJoinTreeNode(AuxiliaryRelation relation, List<String> reserve) {
        super(relation.getRelationId(), "AuxiliaryRelation", relation.getTableDisplayName(), reserve);
        this.support = relation.supportingRelation().getRelationId();
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
        this.alias = relation.tableDisplayName();
        this.source = relation.supportingRelation().getTableDisplayName();
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getSupport() {
        return support;
    }

    public void setSupport(int support) {
        this.support = support;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
