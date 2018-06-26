package org.neo4j.graphalgo.metaPathComputationProcs;

import org.neo4j.graphalgo.impl.metapath.GraphReducer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.regex.Pattern;
import java.util.stream.Stream;

public class GraphReducerProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "algo.graphReducer", mode = Mode.WRITE)
    @Description("algo.graphReducer(goodEdgeTypes:String[], goodNodeTypes:String[]) YIELD success, executionTime" +
            "- Remove all nodes and relationships that are not given as goodEdgeTypes or goodNodeTypes.")
            public Stream<GraphReducerProc.Result> graphReducer(
            @Name(value = "goodEdgeTypes") String goodEdgeTypesString,
            @Name(value = "goodNodeTypes") String goodNodeTypesString) {

        String[] goodEdgeTypes = goodEdgeTypesString.substring(1,goodEdgeTypesString.length()-1).split(Pattern.quote(", "));
        String[] goodNodeTypes = goodNodeTypesString.substring(1,goodNodeTypesString.length()-1).split(Pattern.quote(", "));

        final GraphReducer algo = new GraphReducer(db, log, goodNodeTypes, goodEdgeTypes);

        long startTime = System.currentTimeMillis();
        algo.compute();
        long executionTime = System.currentTimeMillis() - startTime;

        return Stream.of(new GraphReducerProc.Result(true, executionTime));
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
