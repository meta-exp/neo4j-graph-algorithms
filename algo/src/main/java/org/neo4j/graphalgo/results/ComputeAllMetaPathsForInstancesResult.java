package org.neo4j.graphalgo.results;

import java.util.HashMap;
import java.util.HashSet;

public class ComputeAllMetaPathsForInstancesResult {

    public final String metaPaths;

    private ComputeAllMetaPathsForInstancesResult(HashMap<String, HashSet<Integer>> metaPaths) {
        this.metaPaths = "";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsForInstancesResult> {

        private HashMap<String, HashSet<Integer>> metaPaths;

        public void setMetaPaths(HashMap<String, HashSet<Integer>> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsForInstancesResult build() {
            return new ComputeAllMetaPathsForInstancesResult(metaPaths);

        }
    }
}