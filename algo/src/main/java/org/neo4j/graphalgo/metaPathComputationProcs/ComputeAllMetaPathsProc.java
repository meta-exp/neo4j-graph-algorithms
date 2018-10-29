package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metapath.ComputeAllMetaPaths;
import org.neo4j.graphalgo.impl.metapath.labels.LabelImporter;
import org.neo4j.graphalgo.impl.metapath.labels.LabelMapping;
import org.neo4j.graphalgo.results.metaPathComputationResults.ComputeAllMetaPathsResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class ComputeAllMetaPathsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPaths")
    @Description("CALL algo.computeAllMetaPaths(length:int) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' and saves them to a File called 'Precomputed_MetaPaths.txt' \n")

    public Stream<ComputeAllMetaPathsResult> computeAllMetaPaths(
            @Name(value = "length", defaultValue = "5") Long length) throws Exception {

        final HeavyGraph graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        int processorCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);

        LabelMapping labelMapping = LabelImporter.loadMetaData(graph, api);
        final ComputeAllMetaPaths algo = new ComputeAllMetaPaths(graph, labelMapping, length.intValue(),
                    new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt")), executor);
        Map<ComputeAllMetaPaths.MetaPath, Long> result = algo.compute();
        graph.release();
        return result.entrySet().stream().map(e -> new ComputeAllMetaPathsResult(e.getKey(), e.getValue(), labelMapping));
    }
}