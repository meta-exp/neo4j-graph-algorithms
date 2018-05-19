package org.neo4j.graphalgo.api;

import java.util.AbstractMap;
import java.util.Collection;

public interface ArrayGraphInterface {

    int[] getAdjacentNodes(int nodeId);

    int[] getOutgoingNodes(int nodeId);

    int[] getIncomingNodes(int nodeId);

    int getLabel(int nodeId);

    Integer[] getLabels(int nodeId);

    int getEdgeLabel(long nodeId1, long nodeId2);

    Collection<Integer> getAllEdgeLabels();

    Collection<Integer> getAllLabels();

    AbstractMap<Integer, String> getNodeLabelDict();

    AbstractMap<Integer, String> getEdgeLabelDict();
}
