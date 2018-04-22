package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenTypes;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFull;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsBetweenTypesResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

public class ComputeAllMetaPathsSchemaFullProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPathsSchemaFull")
    @Description("CALL algo.computeAllMetaPathsSchemaFull(length:int) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' but dont garuatny their existence' \n")//TODO change description

    public Stream<ComputeAllMetaPathsBetweenTypesResult> ComputeAllMetaPathsSchemaFull(
            @Name(value = "length", defaultValue = "5") String lengthString) throws Exception {
        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsBetweenTypesResult.Builder builder = ComputeAllMetaPathsBetweenTypesResult.builder();

        final ComputeAllMetaPathsSchemaFull algo = new ComputeAllMetaPathsSchemaFull(length, api);
        HashSet<String> metaPaths;
        ComputeAllMetaPathsSchemaFull.Result result = algo.compute();
        metaPaths = result.getFinalMetaPaths();
        HashMap<Integer, String> nodesIDTypeDict = result.getIDTypeNodeDict();
        HashMap <Integer, String> edgesIDTypeDict = result.getIDTypeEdgeDict();
        HashMap<String, Double> metaPathWeightsDict = result.getMetaPathWeightsDict();
        builder.setMetaPaths(metaPaths);
        builder.setNodesIDTypeDict(nodesIDTypeDict);
        builder.setEdgesIDTypeDict(edgesIDTypeDict);
        builder.setMetaPathWeightsDict(metaPathWeightsDict);

        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());

    }
}
