package org.neo4j.graphalgo.results.metaPathComputationResults;

import com.google.gson.Gson;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;


public class ComputeAllMetaPathsBetweenTypesResult {

    public final String metaPaths;
    public final String nodesIDTypeDict;
    public final String edgesIDTypeDict;

    private ComputeAllMetaPathsBetweenTypesResult(HashSet<String> metaPaths, HashMap<Integer, String> nodesIDTypeDict, HashMap<Integer, String> edgesIDTypeDict, HashMap<String, Double> metaPathWeightsDict) {
        Gson gson = new Gson();
        this.metaPaths = gson.toJson(metaPathWeightsDict); //TODO remove metaPaths
        this.nodesIDTypeDict = gson.toJson(nodesIDTypeDict);
        this.edgesIDTypeDict = gson.toJson(edgesIDTypeDict);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ComputeAllMetaPathsBetweenTypesResult> {

        private HashSet<String> metaPaths;
        private HashMap<Integer, String> nodesIDTypeDict;
        private HashMap<Integer, String> edgesIDTypeDict;
        private HashMap<String, Double> metaPathWeightsDict;

        public void setMetaPaths(HashSet<String> metaPaths) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.metaPaths = metaPaths;
        }

        public void setNodesIDTypeDict(HashMap<Integer, String> nodesIDTypeDict) {
            this.nodesIDTypeDict = nodesIDTypeDict;
        }

        public void setEdgesIDTypeDict(HashMap<Integer, String> edgesIDTypeDict) {
            this.edgesIDTypeDict = edgesIDTypeDict;
        }

        public void setMetaPathWeightsDict(HashMap<String, Double> metaPathWeightsDict){
            this.metaPathWeightsDict = metaPathWeightsDict;
        }

        public ComputeAllMetaPathsBetweenTypesResult build() {
            return new ComputeAllMetaPathsBetweenTypesResult(metaPaths, nodesIDTypeDict, edgesIDTypeDict, metaPathWeightsDict);

        }
    }
}