package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.multiTypes.MultiTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

public class MultiTypesProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "algo.multiTypes", mode = Mode.WRITE)
    @Description("algo.multiTypes(edgeType:String, typeLabel:String) YIELD success, executionTime" +
            "- Convert a graph in that labels are nodes to which entities have an edge to " +
            "to a graph where each node has their label in the label attribute.")
    public Stream<Result> multiTypes(
            @Name(value = "edgeType", defaultValue = "/type/object/type") String edgeType,
            @Name(value = "typeLabel", defaultValue = "Type") String typeLabel) throws Exception {

        final MultiTypes algo = new MultiTypes(db, edgeType, typeLabel, log);

        long startTime = System.currentTimeMillis();
        algo.compute();
        long executionTime = System.currentTimeMillis() - startTime;

        return Stream.of(new Result(true, executionTime));
    }


    @Procedure(value = "algo.multiTypesSingleNode", mode = Mode.WRITE)
    @Description("algo.multiTypesSingleNode(nodeId:Number, edgeType:String, typeLabel:String) YIELD success, executionTime" +
            "- Add name of this node as type to all neighbors.")
    public Stream<Result> multiTypesSingleNode(
            @Name(value = "nodeId") Number nodeId,
            @Name(value = "edgeType", defaultValue = "/type/object/type") String edgeType,
            @Name(value = "typeLabel", defaultValue = "Type") String typeLabel) throws Exception {

        final MultiTypes algo = new MultiTypes(db, edgeType, typeLabel, log);
        long startTime = System.currentTimeMillis();
//        try (Transaction transaction = api.beginTx()) {
//            Node node = api.getNodeById(nodeId.longValue());
            algo.updateNodeNeighbors(nodeId.longValue());
//            transaction.success();
//        }
        long executionTime = System.currentTimeMillis() - startTime;

        return Stream.of(new Result(true, executionTime));
    }

    public class Result {
        public boolean success;
        public long executionTime;

        public Result(boolean success, long executionTime) {
            this.success = success;
            this.executionTime = executionTime;
        }
    }
}