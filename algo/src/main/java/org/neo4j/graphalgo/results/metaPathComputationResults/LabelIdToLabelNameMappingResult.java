package org.neo4j.graphalgo.results.metaPathComputationResults;

import com.google.gson.Gson;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.HashMap;

public class LabelIdToLabelNameMappingResult {
    public final String labelIdToLabelNameDict;

    private LabelIdToLabelNameMappingResult(HashMap<Integer, String> labelIdToLabelNameDict) {
        Gson gson = new Gson();
        this.labelIdToLabelNameDict = gson.toJson(labelIdToLabelNameDict);
    }

    public static LabelIdToLabelNameMappingResult.Builder builder() {
        return new LabelIdToLabelNameMappingResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<LabelIdToLabelNameMappingResult> {

        private HashMap<Integer, String> labelIdToLabelNameDict;

        public void setLabelIdToLabelNameDict(HashMap<Integer, String> labelIdToLabelNameDict) {
            this.labelIdToLabelNameDict = labelIdToLabelNameDict;
        }

        public LabelIdToLabelNameMappingResult build() {
            return new LabelIdToLabelNameMappingResult(labelIdToLabelNameDict);

        }
    }
}
