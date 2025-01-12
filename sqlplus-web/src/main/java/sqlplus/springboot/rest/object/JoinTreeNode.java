package sqlplus.springboot.rest.object;

import java.util.List;

public class JoinTreeNode {
    int id;
    String type;
    String alias;
    List<String> reserves;
    List<Integer> hintJoinOrder;

    Long cardinality;

    public JoinTreeNode() {
    }

    public JoinTreeNode(int id, String type, String alias, List<String> reserves, List<Integer> hintJoinOrder, Long cardinality) {
        this.id = id;
        this.type = type;
        this.alias = alias;
        this.reserves = reserves;
        this.hintJoinOrder = hintJoinOrder;
        this.cardinality = cardinality;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getReserves() {
        return reserves;
    }

    public void setReserves(List<String> reserves) {
        this.reserves = reserves;
    }

    public List<Integer> getHintJoinOrder() {
        return hintJoinOrder;
    }

    public void setHintJoinOrder(List<Integer> hintJoinOrder) {
        this.hintJoinOrder = hintJoinOrder;
    }

    public Long getCardinality() { return this.cardinality; }

    public void setCardinality(Long cardinality) { this.cardinality = cardinality; }
}
