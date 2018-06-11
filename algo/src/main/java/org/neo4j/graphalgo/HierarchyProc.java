package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.multiTypes.Hierarchy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

public class HierarchyProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "algo.hierarchy", mode = Mode.WRITE)
    @Description("algo.multiTypes(startNodeId:Number, edgeType:String, nameProperty:String, maxDepth:Number, typeLabel:String) " +
            "YIELD success, executionTime" +
            "- Version 1.0: Creates a type hierarchy of nodes.")
    public Stream<Result> hierarchy(
            @Name(value = "startNodeId") Number startNodeId,
            @Name(value = "maxDepth") Number maxDepth,
            @Name(value = "followLabel", defaultValue = "subclass_of") String followLabel,
            @Name(value = "nameProperty", defaultValue = "label") String nameProperty,
            @Name(value = "typeLabel", defaultValue = "") String typeLabel) {

        final Hierarchy algo = new Hierarchy(db, followLabel, nameProperty, typeLabel, log);

        long startTime = System.currentTimeMillis();
        algo.compute(startNodeId.longValue(), maxDepth.intValue());
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