package org.neo4j.graphalgo.core.heavyweight.Labels;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

public class NullLabeler implements GraphLabeler {

    @Override
    public int getLabel(int nodeId) {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return -1;
    }

    @Override
    public Integer[] getLabels(int nodeId) {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return new Integer[0];
    }

    @Override
    public Collection<Integer> getAllNodeLabels() {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return Collections.emptyList();
    }

    @Override
    public Collection<Integer> getAllEdgeLabels() {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return Collections.emptyList();
    }

    @Override
    public AbstractMap<Integer, String> getNodeLabelDict() {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return new HashMap<>();
    }

    @Override
    public AbstractMap<Integer, String> getEdgeLabelDict() {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return new HashMap<>();
    }

    @Override
    public int getEdgeLabel(long nodeId1, long nodeId2) {
        System.out.println("Warning: Using NullLabeler, LabelMapping is probably not loaded");
        return -1;
    }
}
