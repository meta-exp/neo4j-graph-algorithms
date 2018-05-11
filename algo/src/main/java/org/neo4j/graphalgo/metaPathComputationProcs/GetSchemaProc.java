package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFull;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.GetSchema;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.Pair;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsSchemaFullResult;
import org.neo4j.graphalgo.results.metaPathComputationResults.GetSchemaResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

public class GetSchemaProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.GetSchema")
    @Description("CALL algo.GetSchema() YIELD schema: \n" +
            "return schema as adjacency list \n")

    public Stream<GetSchemaResult> GetSchema() {

        final GetSchemaResult.Builder builder = GetSchemaResult.builder();
        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final GetSchema algo = new GetSchema(graph);

        GetSchema.Result result = algo.compute();
        ArrayList<HashSet<Pair>> schema = result.getSchemaAdjacencies();
        HashMap<Integer, Integer> reverseDictionary = result.getReverseLabelDictionary();
        builder.setSchema(schema);
        builder.setReverseLabelDictionary(reverseDictionary);
        graph.release();

        return Stream.of(builder.build());

    }
}
