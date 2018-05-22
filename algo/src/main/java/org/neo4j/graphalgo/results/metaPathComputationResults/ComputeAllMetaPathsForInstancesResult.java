package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;


public class ComputeAllMetaPathsForInstancesResult {

    public final String metaPaths;

    private ComputeAllMetaPathsForInstancesResult(Vector<String> metaPaths) {
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
            return new ComputeAllMetaPathsForInstancesResult(new Vector<>(metaPaths));
        }
    }
}