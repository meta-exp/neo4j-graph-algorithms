package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsForInstances;
import org.neo4j.graphalgo.results.ComputeAllMetaPathsForInstancesResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
    @Description("CALL algo.computeAllMetaPathsForInstances(length:int) YIELD length: \n" +
            "Compute all metaPaths up to a metapath-length given by 'length' that start with a startNode and end with a endNode and saves them to a File called 'Precomputed_MetaPaths_Instances.txt' \n")

    public Stream<ComputeAllMetaPathsForInstancesResult> computeAllMetaPaths(
            @Name(value = "length", defaultValue = "5") String lengthString) throws IOException {

        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsForInstancesResult.Builder builder = ComputeAllMetaPathsForInstancesResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        final ComputeAllMetaPathsForInstances algo = new ComputeAllMetaPathsForInstances(graph, graph, graph, length);
        HashMap<String, HashSet<Integer>> metaPaths = new HashMap<>();
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}
