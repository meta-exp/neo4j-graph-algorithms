package org.neo4j.graphalgo.core.heavyweight.Labels;

import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.*;

public class LabelMapping implements GraphLabeler {
    private HashMap<Integer, ArrayDeque<Integer>> nodeLabelsMap = new HashMap<>();
    private HashMap<Long, Integer> edgeLabelMap = new HashMap<>();
    private HashMap<Integer, String> nodeLabelToString = new HashMap<>(), edgeLabelToString = new HashMap<>();

    public LabelMapping(){
    }

    @Override
    public int getLabel(int nodeId) {
        ArrayDeque<Integer> labels = getNodeMapping(nodeId);
        if(labels.isEmpty()){
            // Return -1 if this node has no labels
            return -1;
        }
        return labels.getFirst();
    }

    @Override
    public Integer[] getLabels(int nodeId){
        ArrayDeque<Integer> deque = getNodeMapping(nodeId);
        return deque.toArray(new Integer[deque.size()]);
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
    public int getEdgeLabel(int nodeId1, int nodeId2) {
        long combined = RawValues.combineSorted(nodeId1, nodeId2);
        return edgeLabelMap.getOrDefault(combined, -1);
    }

    public void putEdgeMapping(int nodeId1, int nodeId2, int typeId){
        long combined = RawValues.combineSorted(nodeId1, nodeId2);
        edgeLabelMap.put(combined, typeId);
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
