package org.neo4j.graphalgo.results.metaPathComputationResults;

import com.google.gson.Gson;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.Pair;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class GetSchemaResult {

    public final String schema;
    public final String reverseLabelDictionary;

    private GetSchemaResult(ArrayList<HashSet<Pair>> schema, HashMap<Integer, Integer> reverseLabelDictionary) {
        Gson gson = new Gson();
        this.schema = gson.toJson(schema);
        this.reverseLabelDictionary = gson.toJson(reverseLabelDictionary);
    }

    public static GetSchemaResult.Builder builder() {
        return new GetSchemaResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<GetSchemaResult> {

        private ArrayList<HashSet<Pair>> schema;
        private HashMap<Integer, Integer> reverseLabelDictionary;

        public void setSchema(ArrayList<HashSet<Pair>> schema) {
            this.schema = schema;
        }

        public void setReverseLabelDictionary(HashMap<Integer, Integer> reverseLabelDictionary) {
            this.reverseLabelDictionary = reverseLabelDictionary;
        }

        public GetSchemaResult build() {
            return new GetSchemaResult(schema, reverseLabelDictionary);
        }
    }
}
