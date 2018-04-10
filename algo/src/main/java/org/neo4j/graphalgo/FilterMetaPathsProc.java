package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.FilterMetaPaths;
import org.neo4j.graphalgo.results.FilterMetaPathsResult;
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

    public Stream<FilterMetaPathsResult> filterAllMetaPaths(
            @Name(value = "startLabel", defaultValue = "0") String startLabelString,
            @Name(value = "endLabel", defaultValue = "0") String endLabelString) throws Exception {

        final FilterMetaPathsResult.Builder builder = FilterMetaPathsResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final FilterMetaPaths algo = new FilterMetaPaths();
        HashMap<String, Long> filteredMetaPathsDict;
        filteredMetaPathsDict = algo.filter(startLabelString, endLabelString).getFilteredMetaPathsDict();
        builder.setFilteredMetaPathsDict(filteredMetaPathsDict);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}
