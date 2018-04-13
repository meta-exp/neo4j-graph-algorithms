package org.neo4j.graphalgo.api;

import scala.Int;

import java.util.Collection;
import java.util.HashMap;

public interface ArrayGraphInterface {

    int[] getAdjacentNodes(int nodeId);

    int[] getOutgoingNodes(int nodeId);

    int[] getIncomingNodes(int nodeId);

    int getLabel(int nodeId);

    int getEdgeLabel(long nodeId1, long nodeId2);

    Collection<Integer> getAllEdgeLabels();

    Collection<Integer> getAllLabels();

    HashMap<Integer, String> getLabelIdToNameDict();
}
