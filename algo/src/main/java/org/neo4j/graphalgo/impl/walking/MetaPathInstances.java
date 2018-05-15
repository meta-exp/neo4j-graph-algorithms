package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.HashMap;

public class MetaPathInstances extends AbstractWalkAlgorithm {

    private HashMap<Integer, String> labelIdToNameDict;

    public MetaPathInstances(HeavyGraph graph, Log log){
        super(graph, log);
        this.labelIdToNameDict = graph.getLabelIdToNameDict();
    }

    public void doExtraction(int nodeId, int[] previousResults, int[] types, AbstractWalkOutput output){
        int currentNodeType = types[previousResults.length];
        // End this walk if it doesn't match the required types anymore
        if(graph.getLabel(nodeId) != currentNodeType){
            return;
        }

        // If this walk completes the required types, end it and save it
        if(previousResults.length + 1 == types.length){
            // TODO Do something
        }

        int nextEdgeType = types[previousResults.length+1];
        int[] neighbours = graph.getAdjacentNodes(nodeId);
        for(int i = 0; i < neighbours.length; i++){
            int neighbourId = neighbours[i];
            int edgeType = graph.getEdgeLabel(nodeId, neighbourId);

            if(edgeType == nextEdgeType){
                // TODO Do something
            }
        }
    }
}
