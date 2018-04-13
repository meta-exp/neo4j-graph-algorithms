package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.HashMap;

public class LabelIdToLabelNameMapping extends Algorithm<LabelIdToLabelNameMapping> {

    private ArrayGraphInterface arrayGraphInterface;

    public LabelIdToLabelNameMapping(ArrayGraphInterface arrayGraphInterface)
    {
        this.arrayGraphInterface = arrayGraphInterface;
    }

    public LabelIdToLabelNameMapping.Result getLabelIdToLabelNameMapping()
    {
        return new Result(arrayGraphInterface.getLabelIdToNameDict());
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

        HashMap<Integer, String> labelIdToLabelNameDict;
        public Result(HashMap<Integer, String> labelIdToLabelNameDict) {
            this.labelIdToLabelNameDict = labelIdToLabelNameDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashMap<Integer, String> getLabelIdToLabelNameDict() {
            return labelIdToLabelNameDict;
        }
    }
}
