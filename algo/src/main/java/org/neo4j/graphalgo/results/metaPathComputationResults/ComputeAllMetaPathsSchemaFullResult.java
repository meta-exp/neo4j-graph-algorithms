package org.neo4j.graphalgo.results.metaPathComputationResults;

import com.google.gson.Gson;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class ComputeAllMetaPathsSchemaFullResult {

    public final List<String> metaPaths;

    private ComputeAllMetaPathsSchemaFullResult(HashSet<String> metaPaths) {
        this.metaPaths = new ArrayList<>();
        this.metaPaths.addAll(metaPaths);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsSchemaFullResult> {

        private HashSet<String> metaPaths;

        public void setMetaPaths(HashSet<String> metaPaths) {
            this.metaPaths = metaPaths;
        }

        public ComputeAllMetaPathsSchemaFullResult build() {
            return new ComputeAllMetaPathsSchemaFullResult(metaPaths);
        }
    }
}