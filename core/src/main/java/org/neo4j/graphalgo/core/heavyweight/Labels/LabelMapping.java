package org.neo4j.graphalgo.core.heavyweight.Labels;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

public class LabelMapping implements GraphLabeler {
    private HashMap<Integer, ArrayDeque<Integer>> nodeLabelsMap = new HashMap<>();
    private HashMap<long[], Integer> edgeLabelMap = new HashMap<>();
    private HashMap<Integer, String> nodeLabelToString = new HashMap<>(), edgeLabelToString = new HashMap<>();

    public LabelMapping(){
    }

    @Override
    public int getLabel(int nodeId) {
        return getNodeMapping(nodeId).getFirst();
    }

    @Override
    public Integer[] getLabels(int nodeId){
        return (Integer[]) getNodeMapping(nodeId).toArray();
    }

    @Override
    public Collection<Integer> getAllNodeLabels()
    {
        return nodeLabelToString.keySet();
    }

    @Override
    public Collection<Integer> getAllEdgeLabels()
    {
        return edgeLabelToString.keySet();
    }

    @Override
    public AbstractMap<Integer, String> getNodeLabelDict()
    {
        return nodeLabelToString;
    }

    @Override
    public AbstractMap<Integer, String> getEdgeLabelDict() {
        return edgeLabelToString;
    }

    @Override
    public int getEdgeLabel(long nodeId1, long nodeId2) {
        long[] edgeTuple = {nodeId1, nodeId2};
        long[] reversedTuple = {nodeId2, nodeId1};
        return edgeLabelMap.getOrDefault(edgeTuple, edgeLabelMap.getOrDefault(reversedTuple, -1));
    }

    public void putEdgeMapping(long nodeId1, long nodeId2, int typeId){
        long[] edgeTuple = {nodeId1, nodeId2};
        edgeLabelMap.put(edgeTuple, typeId);
    }

    public void putEdgeStringMapping(int typeId, String name){
        edgeLabelToString.put(typeId, name);
    }

    public void addNodeMapping(int nodeId, int typeId){
        if(!nodeLabelsMap.containsKey(nodeId)){
            nodeLabelsMap.put(nodeId, new ArrayDeque<>());
        }
        nodeLabelsMap.get(nodeId).add(typeId);
    }

    private ArrayDeque<Integer> getNodeMapping(int nodeId){
        return nodeLabelsMap.getOrDefault(nodeId, new ArrayDeque<>());
    }

    public void putNodeStringMapping(int typeId, String name){
        nodeLabelToString.put(typeId, name);
    }
}
