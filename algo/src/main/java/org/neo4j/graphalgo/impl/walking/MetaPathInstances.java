package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MetaPathInstances extends AbstractWalkAlgorithm {

    private Map<String, Integer> nodeLabelToString, edgeLabelToString;
    private ThreadPoolExecutor executor;
    private Phaser phaser;


    public MetaPathInstances(HeavyGraph graph, Log log){
        super(graph, log);

        this.nodeLabelToString = getNodeLabelToIdDict();
        this.edgeLabelToString = getEdgeLabelToIdDict();
        this.executor = getExecutor();
        this.phaser = new Phaser();

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

        phaser.register();
        for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            final int executorNodeId = nodeId;
            executor.execute(() -> {
                startExtraction(executorNodeId, emptyArray, types, output);
            });
        }

        phaser.arriveAndAwaitAdvance();
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Thread join timed out");
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

    public void startExtraction(final int nodeId, final int[] previousResults, final int[] types, final AbstractWalkOutput output){
        phaser.register(); // Do not end computation while any extraction is still running
        doExtraction(nodeId, previousResults, types, output);
        phaser.arrive();
    }

    public void doExtraction(final int nodeId, final int[] previousResults, final int[] types, final AbstractWalkOutput output){
        int typeIndex = previousResults.length * 2; // types array contains types for edges and nodes, prev-array contains only node ids
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
                executor.execute(() -> {
                    startExtraction(neighbourId, resultsSoFar, types, output);
                });
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
