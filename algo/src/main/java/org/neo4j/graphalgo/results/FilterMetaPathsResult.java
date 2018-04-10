package org.neo4j.graphalgo.results;

import com.google.gson.Gson;

import java.util.HashMap;

public class FilterMetaPathsResult {

    public final String filteredMetaPathsDict;

    private FilterMetaPathsResult(HashMap<String, Long> filteredMetaPathsDict) {
        Gson gson = new Gson();
        this.filteredMetaPathsDict = gson.toJson(filteredMetaPathsDict);
    }

    public static FilterMetaPathsResult.Builder builder() {
        return new FilterMetaPathsResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<FilterMetaPathsResult> {

        private HashMap<String, Long> filteredMetaPathsDict;

        public void setFilteredMetaPathsDict(HashMap<String, Long> filteredMetaPathsDict) {
            // this.metaPaths =  metaPaths.toArray(new String[metaPaths.size()]);
            this.filteredMetaPathsDict = filteredMetaPathsDict;
        }

        public FilterMetaPathsResult build() {
            return new FilterMetaPathsResult(filteredMetaPathsDict);

        }
    }
}
