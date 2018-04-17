package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenTypes;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsBetweenTypesResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;
import java.util.stream.Stream;

public class ComputeAllMetaPathsBetweenTypesProc {


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsBetweenTypes")
    @Description("CALL algo.computeAllMetaPathsBetweenTypes(length:int, type1:String, type2:String) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' and saves them to a File called 'Precomputed_MetaPaths.txt' \n")//TODO change description

    public Stream<ComputeAllMetaPathsBetweenTypesResult> ComputeAllMetaPathsBetweenTypes(
            @Name(value = "length", defaultValue = "5") String lengthString,
            @Name(value = "type1", defaultValue = "none") String type1,
            @Name(value = "type2", defaultValue = "none") String type2) throws Exception {
        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsBetweenTypesResult.Builder builder = ComputeAllMetaPathsBetweenTypesResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final ComputeAllMetaPathsBetweenTypes algo = new ComputeAllMetaPathsBetweenTypes(length, type1, type2, api);
        HashSet<String> metaPaths;
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}