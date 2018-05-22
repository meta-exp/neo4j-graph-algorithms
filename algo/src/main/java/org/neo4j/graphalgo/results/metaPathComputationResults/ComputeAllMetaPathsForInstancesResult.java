package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;
import java.util.HashSet;


public class ComputeAllMetaPathsForInstancesResult {

    public final String metaPaths;

    private ComputeAllMetaPathsForInstancesResult() {
        this.metaPaths = "";
        //Gson gson = new Gson();
        //this.metaPaths = gson.toJson(metaPaths);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsForInstancesResult> {

        private HashSet<String> metaPaths;

        public void setMetaPaths(HashSet<String> metaPaths) {
           this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsForInstancesResult build() {
            return new ComputeAllMetaPathsForInstancesResult();
        }
    }
}