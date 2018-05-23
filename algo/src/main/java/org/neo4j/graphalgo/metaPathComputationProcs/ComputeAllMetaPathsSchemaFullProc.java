package org.neo4j.graphalgo.metaPathComputationProcs;

import com.carrotsearch.hppc.IntIntHashMap;
import com.google.gson.Gson;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFull;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.Pair;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsSchemaFullResult;
import org.neo4j.graphdb.Transaction;
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
import java.util.Map;
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

    public Stream<ComputeAllMetaPathsSchemaFullResult> ComputeAllMetaPathsSchemaFull(
            @Name(value = "length", defaultValue = "5") String lengthString) throws Exception {
        int length = Integer.valueOf(lengthString);

        final ComputeAllMetaPathsSchemaFullResult.Builder builder = ComputeAllMetaPathsSchemaFullResult.builder();

        org.neo4j.graphdb.Result queryResult;
        try (Transaction tx = api.beginTx()) {
            queryResult = api.execute("CALL algo.GetSchema();");
            tx.success();
        }
        Map<String, Object> row = queryResult.next();
        Gson gson = new Gson();
        ArrayList<HashSet<Pair>> schema =  gson.fromJson((String) row.get("schema"), ArrayList.class);
        HashMap<Integer, Integer> reversedLabelDictionary = gson.fromJson((String) row.get("reverseLabelDictionary"), HashMap.class);

        final ComputeAllMetaPathsSchemaFull algo = new ComputeAllMetaPathsSchemaFull(length, schema, reversedLabelDictionary);
        HashSet<String> metaPaths;
        ComputeAllMetaPathsSchemaFull.Result result = algo.compute();
        metaPaths = result.getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);

        return Stream.of(builder.build());

    }
}
