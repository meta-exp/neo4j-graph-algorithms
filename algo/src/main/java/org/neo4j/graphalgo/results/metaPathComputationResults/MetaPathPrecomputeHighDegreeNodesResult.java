package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.HashMap;
import java.util.HashSet;

public class MetaPathPrecomputeHighDegreeNodesResult {

    public final String metaPaths;

    private MetaPathPrecomputeHighDegreeNodesResult(HashMap<Integer, HashMap<String, HashSet<Integer>>> metaPaths) {
        this.metaPaths = "";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<MetaPathPrecomputeHighDegreeNodesResult> {

        private HashMap<Integer, HashMap<String, HashSet<Integer>>> metaPaths;

        public void setMetaPaths(HashMap<Integer, HashMap<String, HashSet<Integer>>> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public MetaPathPrecomputeHighDegreeNodesResult build() {
            return new MetaPathPrecomputeHighDegreeNodesResult(metaPaths);

        }
    }
}