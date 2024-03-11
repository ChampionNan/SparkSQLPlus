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
	String aggList;

	public TableAggJoinTreeNode(TableAggRelation relation) {
		super(relation.getRelationId(), "TableAggRelation", relation.getTableDisplayName());
		this.source = relation.getTableName();
		this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
		this.alias = relation.tableDisplayName();
		this.aggList = relation.getAggId() + '\n' + relation.concatInside();
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

	public String getAggList() { return aggList; }

	public void setAggList(String aggList) {this.aggList = aggList; }
}
