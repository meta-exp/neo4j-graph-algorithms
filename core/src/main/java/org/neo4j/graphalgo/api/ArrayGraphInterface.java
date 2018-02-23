package org.neo4j.graphalgo.api;

import scala.Int;

import java.util.Collection;

public interface ArrayGraphInterface {

    int[] getAdjacentNodes(int nodeId);

    int[] getOutgoingNodes(int nodeId);

    int[] getIncomingNodes(int nodeId);

    int getLabel(int nodeId);

    Collection<Integer> getAllLabels();



}
