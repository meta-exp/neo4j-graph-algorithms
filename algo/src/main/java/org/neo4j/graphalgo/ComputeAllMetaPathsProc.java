package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.ComputeAllMetaPaths;
import org.neo4j.graphalgo.results.ComputeAllMetaPathsResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;
import java.util.stream.Stream;

public class ComputeAllMetaPathsProc {


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.computeAllMetaPaths")
    @Description("CALL algo.computeAllMetaPaths(length:int, max_label_count:int, max_instance_count:int) YIELD length: \n" +
            "Precomputes all metapaths up to a metapath-length given by 'length' and saves them to a File called 'Precomputed_MetaPaths.txt' \n" +
            "Max_label_count is the amount of different nodetypes in the graph. \n" +
            "Max_instance_count tells how many instances a nodetype can have at most. Set to total amount of nodes in the graph to be sure it works. \n")

    public Stream<ComputeAllMetaPathsResult> computeAllMetaPaths(
            @Name(value = "length", defaultValue = "5") String lengthString, @Name(value = "max_label_count", defaultValue = "30") String max_label_countString, @Name(value = "max_instance_count", defaultValue = "100000") String max_instance_countString) throws Exception {
        int length = Integer.valueOf(lengthString);
        int max_label_count = Integer.valueOf(max_label_countString);
        int max_instance_count = Integer.valueOf(max_instance_countString);
        System.out.println("Given length is: " + String.valueOf(length));

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        final ComputeAllMetaPaths algo = new ComputeAllMetaPaths(graph, graph, graph, graph, length, max_label_count, max_instance_count);
        HashSet<String> metaPaths;
        metaPaths = algo.compute().getFinalMetaPaths();
        builder.setMetaPaths(metaPaths);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}