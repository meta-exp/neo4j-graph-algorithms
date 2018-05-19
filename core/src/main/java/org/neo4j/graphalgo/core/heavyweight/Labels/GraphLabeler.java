package org.neo4j.graphalgo.core.heavyweight.Labels;

import java.util.AbstractMap;
import java.util.Collection;

public interface GraphLabeler {
    public int getLabel(int nodeId);

    public Integer[] getLabels(int nodeId);

    public Collection<Integer> getAllNodeLabels();

    public Collection<Integer> getAllEdgeLabels();

    public AbstractMap<Integer, String> getNodeLabelDict();

    public AbstractMap<Integer, String> getEdgeLabelDict();

    public int getEdgeLabel(long nodeId1, long nodeId2);
}
