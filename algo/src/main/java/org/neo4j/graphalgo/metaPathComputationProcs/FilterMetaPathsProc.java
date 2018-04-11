package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.impl.FilterMetaPaths;
import org.neo4j.graphalgo.results.metaPathComputationResults.MetaPathComputationResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.stream.Stream;

public class FilterMetaPathsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.filterMetaPaths")
    @Description("CALL algo.filterAllMetaPaths(startLabel:int, endLabel:int) YIELD length: \n" +
            "Finds all metaPaths with the specified start and end label and saves them to a File called 'Filtered_MetaPaths.txt' \n")

    public Stream<MetaPathComputationResult> filterAllMetaPaths(
            @Name(value = "startLabel", defaultValue = "0") String startLabelString,
            @Name(value = "endLabel", defaultValue = "0") String endLabelString) throws Exception {

        final MetaPathComputationResult.Builder builder = MetaPathComputationResult.builder();

        final FilterMetaPaths algo = new FilterMetaPaths();
        HashMap<String, Long> filteredMetaPathsDict;
        filteredMetaPathsDict = algo.filter(startLabelString, endLabelString).getFilteredMetaPathsDict();
        builder.setMetaPathsDict(filteredMetaPathsDict);
        return Stream.of(builder.build());
    }
}
