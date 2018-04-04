package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.multiTypes.MultiTypes;
import org.neo4j.graphalgo.results.ComputeAllMetaPathsResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class MultiTypesProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Procedure("algo.multiTypes")
    @Description("CALL algo.multiTypes(edgeType:string, typeLabel:string)" +
            "- Convert a graph in that labels are nodes to which entities have an edge to " +
            "to a graph where each node has their label in the label attribute.")

    public void multiTypes(
            @Name(value = "edgeType", defaultValue = "/type/object/type") String edgeType,
            @Name(value = "typeLabel", defaultValue = "Type") String typeLabel) throws Exception {

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final MultiTypes algo = new MultiTypes(graph, db, edgeType, typeLabel);
        algo.compute();

        graph.release();
    }
}