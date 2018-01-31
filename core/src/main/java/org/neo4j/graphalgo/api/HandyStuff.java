package org.neo4j.graphalgo.api;

import scala.Int;

import java.util.Collection;

public interface HandyStuff {

    int[] getEdges(int nodeId);

    int getNodeOnOtherSide(int nodeId, int edgeId);

    String getLabel(int nodeId);

    Collection<String> getAllLabels();



}
