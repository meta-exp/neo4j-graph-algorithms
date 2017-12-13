package org.neo4j.graphalgo.api;

import scala.Int;

public interface HandyStuff {

    int[] getEdges(int nodeId);

    int getNodeOnOtherSide(int nodeId, int edgeId);

}
