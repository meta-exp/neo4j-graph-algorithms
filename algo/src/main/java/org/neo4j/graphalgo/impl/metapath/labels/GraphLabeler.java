package org.neo4j.graphalgo.impl.metapath.labels;

public interface GraphLabeler {
    short getLabel(int nodeId);

    short[] getLabels(int nodeId);

    short[] getAllNodeLabels();

    short[] getAllEdgeLabels();

    Tokens getLabels();

    Tokens getTypes();

    short getEdgeLabel(int start, int end);
}
