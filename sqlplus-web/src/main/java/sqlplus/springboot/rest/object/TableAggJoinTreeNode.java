package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.TableAggRelation;
import sqlplus.graph.TableScanRelation;

import java.util.List;
import java.util.stream.Collectors;

public class TableAggJoinTreeNode extends JoinTreeNode {
	String source;
	List<String> columns;
	String alias;

	public TableAggJoinTreeNode(TableAggRelation relation) {
		super(relation.getRelationId(), "TableAggRelation");
		this.source = relation.getTableName();
		this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
		this.alias = relation.tableDisplayName();
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
