package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MetaPathInstances extends AbstractWalkAlgorithm {

    private Map<String, Integer> nodeLabelToString, edgeLabelToString;

    public MetaPathInstances(HeavyGraph graph, Log log){
        super(graph, log);

        this.nodeLabelToString = getNodeLabelToIdDict();
        this.edgeLabelToString = getEdgeLabelToIdDict();
    }

    private Map<String, Integer> getNodeLabelToIdDict(){
        Map<Integer, String> nodeLabelIdToNameDict = graph.getNodeLabelDict();
        return nodeLabelIdToNameDict.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    private Map<String, Integer> getEdgeLabelToIdDict(){
        Map<Integer, String> edgeLabelIdToNameDict = graph.getEdgeLabelDict();
        return edgeLabelIdToNameDict.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public Stream<WalkResult> findMetaPathInstances(String metaPath, AbstractWalkOutput output) {
        int[] emptyArray = {};
        int[] types = parseMetaPath(metaPath);

        for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            doExtraction(nodeId, emptyArray, types, output);
        }

        output.endInput();

        return output.getStream();
    }

    private int[] parseMetaPath(String metaPath){
        String[] stringTypes = metaPath.split("%%");
        int[] typeIds = new int[stringTypes.length];

        for(int i = 0; i < stringTypes.length; i++){
            String stringType = stringTypes[i];
            int typeId;
            if(i % 2 == 0){
                typeId = nodeLabelToString.getOrDefault(stringType, -1);
            } else {
                typeId = edgeLabelToString.getOrDefault(stringType, -1);
            }
            typeIds[i] = typeId;
        }
        return typeIds;
    }

    public void doExtraction(int nodeId, int[] previousResults, int[] types, AbstractWalkOutput output){
        int typeIndex = previousResults.length * 2; // types array contains types for edges and nodes, prev-array contains only nodes
        int currentNodeType = types[typeIndex];
        // End this walk if it doesn't match the required types anymore, a label of -1 means no label is attached to this node
        Integer[] labels = graph.getLabels(nodeId);
        if(!Arrays.stream(labels).anyMatch(x -> x == currentNodeType)){
            return;
        }

        int[] resultsSoFar = arrayIntPush(nodeId, previousResults);

        // If this walk completes the required types, end it and save it
        if(typeIndex + 1 == types.length){
            output.addResult(translateIdsToOriginal(resultsSoFar));
            return;
        }

        int nextEdgeType = types[typeIndex + 1];
        int[] neighbours = graph.getAdjacentNodes(nodeId);
        for(int i = 0; i < neighbours.length; i++){
            int neighbourId = neighbours[i];
            int edgeType = graph.getEdgeLabel(nodeId, neighbourId);

            if(edgeType == nextEdgeType){
                doExtraction(neighbourId, resultsSoFar, types, output);
            }
        }
    }

    private static int[] arrayIntPush(int item, int[] oldArray) {
        int len = oldArray.length;
        int[] newArray = new int[len+1];
        System.arraycopy(oldArray, 0, newArray, 0, len);
        newArray[len] = item;

        return newArray;
    }


}
