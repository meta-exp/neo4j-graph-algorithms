package org.neo4j.graphalgo.api;

import scala.Int;

import java.util.Collection;

public interface HandyStuff {

    int[] getAdjecentNodes(int nodeId);

    int[] getOutgoingNodes(int nodeId);

    int[] getIncomingNodes(int nodeId);

    String getLabel(int nodeId);

    Collection<String> getAllLabels();



}
