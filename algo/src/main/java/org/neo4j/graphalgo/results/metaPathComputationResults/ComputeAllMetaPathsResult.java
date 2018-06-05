package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;

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

        private ArrayList<String> metaPaths;

        public void setMetaPaths(ArrayList<String> metaPaths) {
           this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsResult build() {
            return new ComputeAllMetaPathsResult();
        }
    }
}