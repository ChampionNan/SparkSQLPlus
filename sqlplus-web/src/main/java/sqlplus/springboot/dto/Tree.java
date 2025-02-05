package sqlplus.springboot.dto;

import sqlplus.expression.Variable;
import sqlplus.graph.*;

import java.util.*;
import java.util.stream.Collectors;

public class Tree {
    private Map<String, Object> joinTree;
    private int treeHeight;

    public Map<String, Object> getJoinTree() {
        return joinTree;
    }

    public void setJoinTree(Map<String, Object> joinTree) {
        this.joinTree = joinTree;
    }

    public int getTreeHeight() {
        return treeHeight;
    }

    public void setTreeHeight(int treeHeight) {
        this.treeHeight = treeHeight;
    }

    public static Tree fromJoinTree(JoinTree joinTree, Map<String, String> bagRelationNames) {
        Tree tree = new Tree();
        Relation root = joinTree.getRoot();
        Set<JoinTreeEdge> edges = scala.collection.JavaConverters.setAsJavaSet(joinTree.getEdges());
        HashMap<String, Object> rootMap = new HashMap<>();
        List<Relation> children = new ArrayList<>();
        edges.forEach(e -> {
            if (e.getSrc().equals(root)) {
                children.add(e.getDst());
            } else if (e.getDst().equals(root)) {
                children.add(e.getSrc());
            }
        });

        if (root instanceof BagRelation) {
            String bagName = bagRelationNames.get(root.getTableDisplayName());
            rootMap.put("relation", bagName);
            rootMap.put("isBag", true);
            rootMap.put("cardinality", -1);
        } else {
            rootMap.put("relation", root.getTableDisplayName());
            rootMap.put("isBag", false);
            rootMap.put("cardinality", root.getCardinality());
        }
        rootMap.put("variables", new ArrayList<>(scala.collection.JavaConverters.seqAsJavaList(root.getVariableList()).stream().map(Variable::name).collect(Collectors.toList())));
        rootMap.put("children", new ArrayList<HashMap<String, Object>>());
        rootMap.put("leaf", 0);

        final int[] maxLevel = {1};
        children.forEach(c -> {
            int childLevel = visitRelation(c, root, rootMap, edges, bagRelationNames, 2);
            if (childLevel > maxLevel[0])
                maxLevel[0] = childLevel;
        });
        if (((int)(rootMap.get("leaf"))) < 1) {
            rootMap.put("leaf", 1);
        }

        tree.setTreeHeight(maxLevel[0]);
        tree.setJoinTree(rootMap);
        return tree;
    }

    private static int visitRelation(Relation relation, Relation parent, HashMap<String, Object> parentMap, Set<JoinTreeEdge> edges,
                                     Map<String, String> bagRelationNames, int level) {
        HashMap<String, Object> map = new HashMap<>();
        List<Relation> children = new ArrayList<>();
        edges.forEach(e -> {
            if (e.getSrc().equals(relation) && !e.getDst().equals(parent)) {
                children.add(e.getDst());
            } else if (e.getDst().equals(relation) && !e.getSrc().equals(parent)) {
                children.add(e.getSrc());
            }
        });

        ((ArrayList<HashMap<String, Object>>) (parentMap.get("children"))).add(map);

        if (relation instanceof BagRelation) {
            String bagName = bagRelationNames.get(relation.getTableDisplayName());
            map.put("relation", bagName);
            map.put("isBag", true);
            map.put("cardinality", -1);
        } else {
            map.put("relation", relation.getTableDisplayName());
            map.put("isBag", false);
            map.put("cardinality", relation.getCardinality());
        }
        map.put("variables", new ArrayList<>(scala.collection.JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList())));
        map.put("children", new ArrayList<HashMap<String, Object>>());
        map.put("leaf", 0);

        final int[] maxLevel = {level};
        children.forEach(c -> {
            int childLevel = visitRelation(c, relation, map, edges, bagRelationNames, level + 1);
            if (childLevel > maxLevel[0])
                maxLevel[0] = childLevel;
        });
        if (((int) (map.get("leaf"))) < 1) {
            map.put("leaf", 1);
        }

        parentMap.put("leaf", (int) (parentMap.get("leaf")) + (int) (map.get("leaf")));
        return maxLevel[0];
    }
}
