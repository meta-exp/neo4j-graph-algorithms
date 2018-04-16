package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.MetaPathPrecomputeHighDegreeNodes;
import org.neo4j.graphalgo.results.metaPathComputationResults.MetaPathPrecomputeHighDegreeNodesResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

public class MetaPathPrecomputeHighDegreeNodesProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.metaPathPrecomputeHighDegreeNodes")
    @Description("CALL algo.metaPathPrecomputeHighDegreeNodes(length:int, ratioHighDegreeNodes:float) YIELD length: \n" +
            "Compute for a certain amount of nodes, given by 'ratioHighDegreeNodes', with the highest degrees their meta-paths up to a meta-path-length given by 'length' and save their nodeID, meta-paths and the end-nodes of these meta-paths in a file called 'Precomputed_MetaPaths_HighDegree.txt' \n")

    public Stream<MetaPathPrecomputeHighDegreeNodesResult> computeAllMetaPaths(
            @Name(value = "length", defaultValue = "5") String lengthString,
            @Name(value = "ratioHighDegreeNodes", defaultValue = "0.0000001") String ratioHighDegreeNodesString) throws IOException, InterruptedException {

        int length = Integer.valueOf(lengthString);
        float ratioHighDegreeNodes = Float.valueOf(ratioHighDegreeNodesString);

        final MetaPathPrecomputeHighDegreeNodesResult.Builder builder = MetaPathPrecomputeHighDegreeNodesResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        final MetaPathPrecomputeHighDegreeNodes algo = new MetaPathPrecomputeHighDegreeNodes(graph, graph, graph, length, ratioHighDegreeNodes, api);
        HashMap<Integer, HashMap<String, HashSet<Integer>>> metaPaths = new HashMap<>();
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}
