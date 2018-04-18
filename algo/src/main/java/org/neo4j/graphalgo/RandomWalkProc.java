package org.neo4j.graphalgo;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Rand;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.GettingStarted;
import org.neo4j.graphalgo.impl.RandomWalk;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class RandomWalkProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "randomWalk", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at the given start node.")
    public Stream<RandomWalkResult> randomWalk(@Name("nodeId") long nodeId,
                                                          @Name("steps") long steps,
                                                          @Name("walks") long walks) {

        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.randomWalk(new RandomWalk.RandomWalkDatabaseOutput(walker), nodeId, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "randomWalk_fileOutput", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at the given start node.")
    public Stream<RandomWalkResult> randomWalk(@Name("nodeId") long nodeId,
                                               @Name("steps") long steps,
                                               @Name("walks") long walks,
                                               @Name("filePath") String filePath) throws IOException {
        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.randomWalk(new RandomWalk.RandomWalkNodeDirectFileOutput(filePath), nodeId, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "multiRandomWalk", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at multiple random start nodes of the given type. " +
            "If not type is given any node is chosen.")
    public Stream<RandomWalkResult> multiRandomWalk(@Name("steps") long steps,
                                                    @Name("walks") long walks,
                                                    @Name(value = "type", defaultValue = "") String type) {
        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.multiRandomWalk(new RandomWalk.RandomWalkDatabaseOutput(walker), steps, walks, type);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "multiRandomWalk_fileOutput", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at multiple random start nodes of the given type. " +
            "If not type is given any node is chosen.")
    public Stream<RandomWalkResult> multiRandomWalk(@Name("steps") long steps,
                                                    @Name("walks") long walks,
                                                    @Name("filePath") String filePath,
                                                    @Name(value = "type", defaultValue = "") String type) throws IOException {
        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.multiRandomWalk(new RandomWalk.RandomWalkNodeDirectFileOutput(filePath), steps, walks, type);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "allNodesRandomWalk", mode = Mode.READ)
    @Description("Starts random walks from every node in the graph. Specify the steps for each random walks " +
            "and the number of walks per node.")
    public Stream<RandomWalkResult> allNodesRandomWalk(@Name("steps") long steps,
                                                       @Name("walks") long walks) {
        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.allNodesRandomWalk(new RandomWalk.RandomWalkDatabaseOutput(walker), steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "allNodesRandomWalk_fileOutput", mode = Mode.READ)
    @Description("Starts random walks from every node in the graph. Specify the steps for each random walks " +
            "and the number of walks per node.")
    public Stream<RandomWalkResult> allNodesRandomWalk(@Name("steps") long steps,
                                                       @Name("walks") long walks,
                                                       @Name("filePath") String filePath) throws IOException {
        RandomWalk walker = getRandomWalk();
        Stream<RandomWalkResult> stream = walker.allNodesRandomWalk(new RandomWalk.RandomWalkNodeDirectFileOutput(filePath), steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    private HeavyGraph getGraph(){
        AllocationTracker tracker = AllocationTracker.create();
        HeavyGraph graph = (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutNodeProperties()
                .withoutExecutorService()
                .withoutRelationshipWeights()
                .withAllocationTracker(tracker)
                .load(HeavyGraphFactory.class);

        return graph;
    }

    private RandomWalk getRandomWalk(){
        HeavyGraph graph = getGraph();
        return new RandomWalk(graph, log, db);
    }

    public static class RandomWalkResult {
        public Path path;

        public RandomWalkResult(Path path) {
            this.path = path;
        }
    }

    public static class RandomPath implements Path {

        private ArrayList<Node> nodes;
        private ArrayList<Relationship> relationships;

        public RandomPath(int size) {
            nodes = new ArrayList<>(size);
            relationships = new ArrayList<>(Math.max(0, size - 1)); // for empty paths
        }

        public void addNode(Node node) {
            nodes.add(node);
        }

        public void addRelationship(Relationship relationship) {
            relationships.add(relationship);
        }

        @Override
        public Node startNode() {
            return nodes.get(0);
        }

        @Override
        public Node endNode() {
            return nodes.get(nodes.size() - 1);
        }

        @Override
        public Relationship lastRelationship() {
            return relationships.get(relationships.size() - 1);
        }

        @Override
        public Iterable<Relationship> relationships() {
            return relationships;
        }

        @Override
        public Iterable<Relationship> reverseRelationships() {
            ArrayList<Relationship> reverse = new ArrayList<>(relationships);
            Collections.reverse(reverse);
            return reverse;
        }

        @Override
        public Iterable<Node> nodes() {
            return nodes;
        }

        @Override
        public Iterable<Node> reverseNodes() {
            ArrayList<Node> reverse = new ArrayList<>(nodes);
            Collections.reverse(reverse);
            return reverse;
        }

        @Override
        public int length() {
            return nodes.size();
        }

        @Override
        public String toString() {
            return nodes.toString();
        }

        @Override
        public Iterator<PropertyContainer> iterator() {
            //TODO ???????
            return null;
        }
    }

}
