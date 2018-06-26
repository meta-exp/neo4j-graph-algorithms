package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;
import java.util.List;

public class ComputeAllMetaPathsResult {

    public final String metaPaths;

    private ComputeAllMetaPathsResult() {
        this.metaPaths = "";
        //Gson gson = new Gson();
        //this.metaPaths = gson.toJson(metaPaths);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsResult> {

        private List<String> metaPaths;

        public void setMetaPaths(List<String> metaPaths) {
           this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsResult build() {
            return new ComputeAllMetaPathsResult();
        }
    }
}