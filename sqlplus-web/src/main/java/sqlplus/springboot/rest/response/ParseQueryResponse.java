package sqlplus.springboot.rest.response;

import sqlplus.springboot.rest.object.*;

import java.util.ArrayList;
import java.util.List;

public class ParseQueryResponse {
    private String ddl;
    private String query;
    List<Table> tables = new ArrayList<>();
    List<JoinTree> joinTrees = new ArrayList<>();
    List<Computation> computations = new ArrayList<>();
    List<String> outputVariables = new ArrayList<>();
    List<String> groupByVariables = new ArrayList<>();
    List<Aggregation> aggregations = new ArrayList<>();
    TopK topK = null;
    boolean isFull;

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public List<JoinTree> getJoinTrees() {
        return joinTrees;
    }

    public void setJoinTrees(List<JoinTree> joinTrees) {
        this.joinTrees = joinTrees;
    }

    public List<Computation> getComputations() {
        return computations;
    }

    public void setComputations(List<Computation> computations) {
        this.computations = computations;
    }

    public List<String> getOutputVariables() {
        return outputVariables;
    }

    public void setOutputVariables(List<String> outputVariables) {
        this.outputVariables = outputVariables;
    }

    public List<String> getGroupByVariables() {
        return groupByVariables;
    }

    public void setGroupByVariables(List<String> groupByVariables) {
        this.groupByVariables = groupByVariables;
    }

    public List<Aggregation> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<Aggregation> aggregations) {
        this.aggregations = aggregations;
    }

    public TopK getTopK() {
        return topK;
    }

    public void setTopK(TopK topK) {
        this.topK = topK;
    }

    public boolean isFull() {
        return isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public void setQuery(String query) { query =  query; }

    public String getQuery() { return query; }

    public void setDdl(String ddl) { this.ddl = ddl; }

    public String getDdl() { return ddl; }
}


