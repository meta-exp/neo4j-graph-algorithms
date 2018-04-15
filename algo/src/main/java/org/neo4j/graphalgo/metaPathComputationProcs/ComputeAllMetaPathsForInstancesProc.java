package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsForInstances;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ComputeAllMetaPathsForInstancesProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsForInstances")
    @Description("CALL algo.computeAllMetaPathsForInstances(startNodes:long[], endNodes:long[], length:int) YIELD length: \n" +
            "Compute all metaPaths up to a metapath-length given by 'length' that start with a startNode and end with a endNOde and saves them to a File called 'Precomputed_MetaPaths_Instances.txt' \n")

    public Stream<ComputeAllMetaPathsResult> computeAllMetaPaths(
            @Name(value = "startNodes", defaultValue = "{}") String startNodesString,
            @Name(value = "endNodes", defaultValue = "{}") String endNodesString,
            @Name(value = "length", defaultValue = "5") String lengthString) throws IOException {

        int length = Integer.valueOf(lengthString);

        String[] endNodesAsStrings = endNodesString.substring(1,endNodesString.length()-1).split(Pattern.quote(", "));
        Long[] endNodes = new Long[endNodesAsStrings.length];
        for (int i = 0; i < endNodesAsStrings.length; i++) {
            endNodes[i] = Long.parseLong(endNodesAsStrings[i]);
        }

        String[] startNodesAsStrings = startNodesString.substring(1,startNodesString.length()-1).split(Pattern.quote(", "));
        Long[] startNodes = new Long[startNodesAsStrings.length];
        for (int i = 0; i < startNodesAsStrings.length; i++) {
            startNodes[i] = Long.parseLong(startNodesAsStrings[i]);
        }

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        HashSet<Integer> convertedEndNodes = new HashSet<>();//converting in the proc allows for easier testing
        convertIds(graph, endNodes, convertedEndNodes);
        List<Integer> startNodeList = new ArrayList<>(convertedEndNodes);

        HashSet<Integer> convertedStartNodes = new HashSet<>();
        convertIds(graph, startNodes, convertedStartNodes);
        List<Integer> endNodeList = new ArrayList<>(convertedStartNodes);

        final ComputeAllMetaPathsForInstances algo = new ComputeAllMetaPathsForInstances(graph, graph, length, startNodeList, endNodeList);
        HashSet<String> metaPaths;
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }

    public void convertIds(IdMapping idMapping, Long[] incomingIds, HashSet<Integer> convertedIds) {
        for (long id : incomingIds) {
            convertedIds.add(idMapping.toMappedNodeId(id));
        }
    }
}
