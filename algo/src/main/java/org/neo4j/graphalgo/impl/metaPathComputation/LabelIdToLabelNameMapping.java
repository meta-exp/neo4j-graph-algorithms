package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.AbstractMap;
import java.util.HashMap;

public class LabelIdToLabelNameMapping extends Algorithm<LabelIdToLabelNameMapping> {

    private ArrayGraphInterface arrayGraphInterface;

    public LabelIdToLabelNameMapping(ArrayGraphInterface arrayGraphInterface)
    {
        this.arrayGraphInterface = arrayGraphInterface;
    }

    public LabelIdToLabelNameMapping.Result getLabelIdToLabelNameMapping()
    {
        return new Result(arrayGraphInterface.getNodeLabelDict());
    }

    @Override
    public LabelIdToLabelNameMapping me() { return this; }

    @Override
    public LabelIdToLabelNameMapping release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        AbstractMap<Integer, String> labelIdToLabelNameDict;
        public Result(AbstractMap<Integer, String> labelIdToLabelNameDict) {
            this.labelIdToLabelNameDict = labelIdToLabelNameDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public AbstractMap<Integer, String> getLabelIdToLabelNameDict() {
            return labelIdToLabelNameDict;
        }
    }
}
