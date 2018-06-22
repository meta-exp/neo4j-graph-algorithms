package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenInstances;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class ComputeAllMetaPathsBetweenInstancesProc {


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsBetweenInstances")
    @Description("CALL algo.computeAllMetaPathsBetweenInstances(length:int, nodePairSkipProbability:float, edgeSkipProbability:float, {graph: 'my-graph'}) YIELD length: \n" +
            "Precomputes meta paths between all nodes connected by a edge up to a metapath-length given by 'length' and saves them to a file for each node pair." +
            "'nodePairSkipProbability' specifies the probability of skipping one pair of directly connected nodes and 'edgeSkipProbability' specifies the probability to skip an " +
            "edge in the recursive search for matching meta-paths\n")

    public Stream<ComputeAllMetaPathsResult> computeAllMetaPathsBetweenInstances(
            @Name(value = "length", defaultValue = "5") Long length,
            @Name(value = "nodePairSkipProbability", defaultValue = "0") Double nodePairSkipProbability,
            @Name(value = "edgeSkipProbability", defaultValue = "0") Double edgeSkipProbability,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        log.info("Loading the graph...");
        graph = (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .init(log, null, null, configuration)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);
        log.info("Graph loaded.");


        final ComputeAllMetaPathsBetweenInstances algo = new ComputeAllMetaPathsBetweenInstances(graph, length.intValue(), log, nodePairSkipProbability.floatValue(),
                edgeSkipProbability.floatValue());
        log.info("Starting meta-path computation...");
        algo.compute();
        log.info("Finished meta-path computation.");
        graph.release();
        return Stream.of(builder.build());
    }
}