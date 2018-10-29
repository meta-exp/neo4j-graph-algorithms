package org.neo4j.graphalgo.impl.metapath.labels;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongShortHashMap;
import com.carrotsearch.hppc.LongShortMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.storageengine.api.Token;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class LabelMapping implements GraphLabeler {
    private static final short[] NO_LABELS = new short[0];
    private final Tokens labels;
    private final Tokens types;

    private IntObjectMap<short[]> nodeLabelsMap = new IntObjectHashMap<>();
    private LongShortMap edgeLabelMap = new LongShortHashMap();

    public LabelMapping(Iterator<Token> labels, Iterator<Token> types){
        this.labels = new Tokens(labels);
        this.types = new Tokens(types);
    }

    @Override
    public short getLabel(int nodeId) {
        short[] labels = getNodeMapping(nodeId);
        if(labels.length == 0){
            return -1;
        }
        return labels[0];
    }

    public void forEachNode(Consumer<IntObjectCursor<short[]>> callback) {
        nodeLabelsMap.forEach(callback);
    }
    @Override
    public short[] getLabels(int nodeId){
        return getNodeMapping(nodeId);
    }

    @Override
    public short[] getAllNodeLabels()
    {
        return labels.ids;
    }

    @Override
    public short[] getAllEdgeLabels()
    {
        return types.ids;
    }

    @Override
    public Tokens getLabels()
    {
        return labels;
    }

    @Override
    public Tokens getTypes() {
        return types;
    }

    @Override
    public short getEdgeLabel(int start, int end) {
        long combined = RawValues.combineSorted(start, end);
        return edgeLabelMap.getOrDefault(combined, (short)-1);
    }

    public void putEdgeMapping(int nodeId1, int nodeId2, short typeId){
        long combined = RawValues.combineSorted(nodeId1, nodeId2);
        edgeLabelMap.put(combined, typeId);
    }

    public void addNodeMapping(int nodeId, short[] types){
        nodeLabelsMap.put(nodeId,types);
    }

    private short[] getNodeMapping(int nodeId){
        return nodeLabelsMap.getOrDefault(nodeId, NO_LABELS);
    }
}
