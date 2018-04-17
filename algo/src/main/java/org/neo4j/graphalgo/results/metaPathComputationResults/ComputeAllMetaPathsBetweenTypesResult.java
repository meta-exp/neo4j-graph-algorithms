package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.HashSet;
import java.util.Vector;


public class ComputeAllMetaPathsBetweenTypesResult {

    public final String metaPaths;

    private ComputeAllMetaPathsBetweenTypesResult(Vector<String> metaPaths) {
        this.metaPaths = "";
        //Gson gson = new Gson();
        //this.metaPaths = gson.toJson(metaPaths);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsBetweenTypesResult> {

        private HashSet<String> metaPaths;

        public void setMetaPaths(HashSet<String> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsBetweenTypesResult build() {
            return new ComputeAllMetaPathsBetweenTypesResult(new Vector<>(metaPaths));

        }
    }
}