package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.GettingStarted;
import org.neo4j.graphalgo.results.metaPathComputationResults.GettingStartedResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class GettingStartedProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.gettingStarted.stream")
    @Description("CALL algo.gettingStarted.stream({defaultValue:false, concurrency:4}) " +
            "YIELD hasEdge - yields a stream of {hasEdge}")

    public Stream<GettingStarted.Result> gettingStartedStream(
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutRelationshipWeights()
                .withConcurrency(configuration.getConcurrency())
                .withAllocationTracker(tracker)
                .load(configuration.getGraphImpl());

        final GettingStarted algo = new GettingStarted(graph);
        algo.compute();
        graph.release();
        return algo.resultStream();
    }

    @Procedure("algo.gettingStarted")
    @Description("CALL algo.gettingStarted({defaultValue:false, concurrency:4}) " +
            "YIELD loadMillis, writeMillis, computeMillis, hasEdge - yields evaluation details")

    public Stream<GettingStartedResult> gettingStarted(
            @Name(value = "config", defaultValue = "{}")  Map<String, Object> config) {

        final GettingStartedResult.Builder builder = GettingStartedResult.builder();
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withConcurrency(configuration.getConcurrency())
                    .withAllocationTracker(tracker)
                    .load(configuration.getGraphImpl());
        }

        final GettingStarted algo = new GettingStarted(graph);
        builder.timeEval(algo::compute);

        return Stream.of(builder.build());
    }

}
