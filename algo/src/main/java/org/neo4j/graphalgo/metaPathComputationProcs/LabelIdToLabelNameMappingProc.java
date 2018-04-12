package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.LabelIdToLabelNameMapping;
import org.neo4j.graphalgo.results.metaPathComputationResults.LabelIdToLabelNameMappingResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;
import java.util.HashMap;
import java.util.stream.Stream;

public class LabelIdToLabelNameMappingProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.getLabelIdToLabelNameMapping")
    @Description("CALL algo.getLabelIdToLabelNameMapping() YIELD mapping: HashMap<Integer, String> \n" +
            "Returns the mapping from labelIds to LabelNames as a HashMap \n")

    public Stream<LabelIdToLabelNameMappingResult> getLabelIdToLabelNameMapping() throws Exception {

        final LabelIdToLabelNameMappingResult.Builder builder = LabelIdToLabelNameMappingResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        final LabelIdToLabelNameMapping algo = new LabelIdToLabelNameMapping(graph);
        HashMap<Integer, String> labelMapping;
        labelMapping = algo.getLabelIdToLabelNameMapping().getLabelIdToLabelNameDict();
        builder.setLabelIdToLabelNameDict(labelMapping);
        graph.release();
        //return algo.resultStream();
        //System.out.println(Stream.of(builder.build()));
        return Stream.of(builder.build());
    }
}
