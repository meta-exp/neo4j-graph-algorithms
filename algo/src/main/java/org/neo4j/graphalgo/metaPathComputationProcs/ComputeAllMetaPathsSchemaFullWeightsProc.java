package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.impl.metapath.ComputeAllMetaPathsSchemaFullWeights;
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

public class ComputeAllMetaPathsSchemaFullWeightsProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.ComputeAllMetaPathsSchemaFullWeights")
    @Description("CALL algo.ComputeAllMetaPathsSchemaFullWeights(length:int) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' but dont garuatny their existence' \n")//TODO change description

    public Stream<ComputeAllMetaPathsBetweenTypesResult> ComputeAllMetaPathsSchemaFullWeights(
            @Name(value = "length", defaultValue = "5") String lengthString) throws Exception {
        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsBetweenTypesResult.Builder builder = ComputeAllMetaPathsBetweenTypesResult.builder();

        final ComputeAllMetaPathsSchemaFullWeights algo = new ComputeAllMetaPathsSchemaFullWeights(length, api);
        HashSet<String> metaPaths;
        ComputeAllMetaPathsSchemaFullWeights.Result result = algo.compute();
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
