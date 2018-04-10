package org.neo4j.graphalgo.results.metaPathComputationResults;

import com.google.gson.Gson;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.HashMap;

public class MetaPathComputationResult {

    public final String metaPathsDict;

    private MetaPathComputationResult(HashMap<String, Long> metaPathsDict) {
        Gson gson = new Gson();
        this.metaPathsDict = gson.toJson(metaPathsDict);
    }

    public static MetaPathComputationResult.Builder builder() {
        return new MetaPathComputationResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<MetaPathComputationResult> {

        private HashMap<String, Long> metaPathsDict;

        public void setMetaPathsDict(HashMap<String, Long> metaPathsDict) {
            this.metaPathsDict = metaPathsDict;
        }

        public MetaPathComputationResult build() {
            return new MetaPathComputationResult(metaPathsDict);
        }
    }
}
