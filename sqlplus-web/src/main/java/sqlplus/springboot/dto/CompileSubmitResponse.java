package sqlplus.springboot.dto;


import java.util.List;
import java.util.Map;

public class CompileSubmitResponse {
    private List<Candidate> candidates;

    private List<Double> cost;

    private List<List<NodeStat>> nodeStatMap;

    public List<Candidate> getCandidates() {
        return candidates;
    }

    public void setCandidates(List<Candidate> candidates) {
        this.candidates = candidates;
    }

    public List<Double> getCost() {
        return cost;
    }

    public void setCost(List<Double> cost) {
        this.cost = cost;
    }

    public List<List<NodeStat>> getNodeStatMap() { return nodeStatMap; }

    public void setNodeStatMap(List<List<NodeStat>> nodeStatMap) { this.nodeStatMap = nodeStatMap; }
}
