package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.stream.IntStream;
import java.util.stream.Stream;


public class GettingStarted extends Algorithm<GettingStarted> {

    private Graph graph;
    private boolean hasEdge;

    public GettingStarted(Graph graph) {
        this.graph = graph;
        this.hasEdge = false;
    }

    public Result compute() {
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.BOTH, (source, target, id) -> {hasEdge = true;
                return false;});
            return !hasEdge;
        });
        return new Result(this.hasEdge);
    }

    public Stream<GettingStarted.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new Result(hasEdge));
    }

    @Override
    public  GettingStarted me() { return this; }

    @Override
    public GettingStarted release() {
        graph = null;
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final boolean hasEdge;

        public Result(boolean hasEdge) {
            this.hasEdge = hasEdge;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "hasEdge=" + hasEdge +
                    '}';
        }
    }
}
