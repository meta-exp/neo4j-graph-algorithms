package org.neo4j.graphalgo.api;

import scala.Int;

import java.util.Collection;
import java.util.HashMap;

public interface ArrayGraphInterface {

    int[] getAdjacentNodes(int nodeId);

    int[] getOutgoingNodes(int nodeId);

    long nodeCount();

    int[] getIncomingNodes(int nodeId);

    int getLabel(int nodeId);

    Collection<Integer> getAllLabels();

    double valueOf(final int nodeId, final double defaultValue);

    HashMap<Integer, String> getLabelIdToNameDict();
}
