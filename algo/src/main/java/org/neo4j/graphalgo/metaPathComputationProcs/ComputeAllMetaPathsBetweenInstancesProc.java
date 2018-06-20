package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenInstances;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class ComputeAllMetaPathsBetweenInstancesProc {


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsBetweenInstances")
    @Description("CALL algo.computeAllMetaPathsBetweenInstances(length:int) YIELD length: \n" +
            "Precomputes metapaths between all nodes connected by a edge up to a metapath-length given by 'length' and saves them to a file for each node pair \n")

    public Stream<ComputeAllMetaPathsResult> computeAllMetaPathsBetweenInstances(
            @Name(value = "length", defaultValue = "5") Long length) throws Exception {

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        log.info("Loading the graph...");
        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);
        log.info("Graph loaded.");


        final ComputeAllMetaPathsBetweenInstances algo = new ComputeAllMetaPathsBetweenInstances(graph, length.intValue(), log);
        log.info("Starting meta-path computation...");
        algo.compute();
        log.info("Finished meta-path computation.");
        graph.release();
        return Stream.of(builder.build());
    }
}