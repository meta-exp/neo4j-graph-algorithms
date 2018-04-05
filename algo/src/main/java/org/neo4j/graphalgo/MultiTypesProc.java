package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.multiTypes.MultiTypes;
import org.neo4j.graphalgo.results.ComputeAllMetaPathsResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

public class MultiTypesProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Procedure(value = "algo.multiTypes", mode = Mode.WRITE)
    @Description("algo.multiTypes(edgeType:String, typeLabel:String) YIELD success" +
            "- Convert a graph in that labels are nodes to which entities have an edge to " +
            "to a graph where each node has their label in the label attribute.")
    public Stream<Boolean> multiTypes(
            @Name(value = "edgeType", defaultValue = "/type/object/type") String edgeType,
            @Name(value = "typeLabel", defaultValue = "Type") String typeLabel) throws Exception {

        final ComputeAllMetaPathsResult.Builder builder = ComputeAllMetaPathsResult.builder();

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .withDirection(Direction.OUTGOING)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        final MultiTypes algo = new MultiTypes(graph, db, edgeType, typeLabel);
        algo.compute();

        graph.release();

        return Stream.of(true);
    }
}