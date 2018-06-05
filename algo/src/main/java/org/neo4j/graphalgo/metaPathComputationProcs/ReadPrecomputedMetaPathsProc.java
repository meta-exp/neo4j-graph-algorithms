package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.impl.metaPathComputation.ReadPrecomputedMetaPaths;
import org.neo4j.graphalgo.results.metaPathComputationResults.MetaPathComputationResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.stream.Stream;

public class ReadPrecomputedMetaPathsProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.readPrecomputedMetaPaths")
    @Description("CALL algo.readPrecomputedMetaPaths(filePath: String) YIELD metaPaths: \n" +
            "Reads the metaPaths and their counts from the file 'filePath' \n")

    public Stream<MetaPathComputationResult> readMetaPaths (
            @Name(value = "filePath", defaultValue = "Precomputed_MetaPaths") String filePath) throws FileNotFoundException {

        final MetaPathComputationResult.Builder builder = MetaPathComputationResult.builder();

        final ReadPrecomputedMetaPaths algo = new ReadPrecomputedMetaPaths();
        HashMap<String, Long> metaPathsDict;
        metaPathsDict = algo.readMetaPaths(filePath).getMetaPathsDict();
        builder.setMetaPathsDict(metaPathsDict);
        return Stream.of(builder.build());
    }

}
