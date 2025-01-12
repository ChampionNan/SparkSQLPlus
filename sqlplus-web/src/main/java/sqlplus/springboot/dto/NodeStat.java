package sqlplus.springboot.dto;

public class NodeStat {
    private int id;
    private String alias;
    private double trueSize;
    private double estimateSize;

    // 构造函数
    public NodeStat(int id, String alias, double trueSize, double estimateSize) {
        this.id = id;
        this.alias = alias;
        this.trueSize = trueSize;
        this.estimateSize = estimateSize;
    }

    // Getter 和 Setter 方法
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public double getTrueSize() {
        return trueSize;
    }

    public void setTrueSize(double trueSize) {
        this.trueSize = trueSize;
    }

    public double getEstimateSize() {
        return estimateSize;
    }

    public void setEstimateSize(double estimateSize) {
        this.estimateSize = estimateSize;
    }
}