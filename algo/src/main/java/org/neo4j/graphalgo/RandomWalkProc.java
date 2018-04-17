package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.GettingStarted;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class RandomWalkProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.randomWalk.stream")
    @Description("CALL algo.randomWalk.stream(startNode:int, endNode:int, {defaultValue:false, concurrency:4}) " +
            "YIELD hasEdge - yields a stream of {hasEdge}")

    public Stream<GettingStarted.Result> randomWalkStream(
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

}
