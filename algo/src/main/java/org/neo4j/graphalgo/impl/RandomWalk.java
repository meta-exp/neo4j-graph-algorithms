package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.RandomWalkProc;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import javax.management.relation.Relation;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RandomWalk {


    public static Random random = new Random();

    private ArrayGraphInterface arrayGraphInterface;
    private Degrees degrees;
    private IdMapping idMapping;
    private Log log;
    private HeavyGraph graph;
    private GraphDatabaseService db;

    public RandomWalk(HeavyGraph graph, Log log, GraphDatabaseService db){

        this.idMapping = graph;
        this.arrayGraphInterface = graph;
        this.degrees = graph;
        this.graph = graph;
        this.log = log;
        this.db = db;
    }

    public Stream<RandomWalkProc.RandomWalkResult> randomWalk(AbstractWalkOutput output, long nodeId, long steps, long walks) {
        Stream<Integer> stream = Stream.generate(() -> getMappedId(nodeId)).limit(walks);

        randomWalks(output, stream, walks, steps);

        return output.getStream();
    }

    public Stream<RandomWalkProc.RandomWalkResult> multiRandomWalk(AbstractWalkOutput output, long steps, long walks, String type) {
        Stream<Integer> startNodeIdStream;
        if (type.isEmpty()) {
            startNodeIdStream = randomNodesFromAllNodes((int) walks);
        } else {
            startNodeIdStream = Stream.empty();
//            startNodes = randomNodesFromType(type, walks);
        }

        randomWalks(output, startNodeIdStream, walks, steps);

        return output.getStream();
    }

    public Stream<RandomWalkProc.RandomWalkResult> allNodesRandomWalk(AbstractWalkOutput output, long steps, long walks) {
        // TODO: find out why sometimes not all nodes are visited.

        long nodeCount = graph.nodeCount();
        long totalWalks = nodeCount * walks;

        Stream<Integer> stream = Stream.empty();
        for (int i = 0; i < walks; i++) {
            Stream<Integer> nodeIdStream = IntStream.range(0, (int) graph.nodeCount()).boxed();

            stream = Stream.concat(stream, nodeIdStream);
        }

        randomWalks(output, stream, totalWalks, steps);

        return output.getStream();
    }


    private Stream<Integer> randomNodesFromAllNodes(int walks) {
        int nodeCount = (int) graph.nodeCount(); // data type of mapped id is int, so casting shouldn't be a problem
        return Stream.generate(() -> random.nextInt(nodeCount)).limit(walks);
    }

//TODO
//    private List<Node> randomNodesFromType(String type, long walks) {
//        long nodeCount = getNodeCount();
//
//        return randomNodesFromIterator(db.findNodes(Label.label(type)), listRandomNumbers((int) nodeCount, walks));
//    }


    private void randomWalks(AbstractWalkOutput output, Stream<Integer> nodeStream, long numberOfElements, long steps) {
        int cores = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores * 4);
        executor.setCorePoolSize(cores * 4);

        long startTime = System.nanoTime();

        nodeStream.forEach((nodeId)->{
            executor.execute(() -> {
                try {
                    long[][] pathIds = doRandomWalk(nodeId, (int) steps);
                    long numberOfResults = output.numberOfResults() + 1;
                    // log progress ------
                    if(numberOfResults % 50000 == 2500){ //get a better estimate after 2500 walks
                        long progress = (numberOfResults*1000) / (numberOfElements*10); // dont convert to float but keep precision
                        long remainingTime = estimateRemainingTime(startTime, numberOfResults, numberOfElements);
                        long remainingMinutes = TimeUnit.MINUTES.convert(remainingTime, TimeUnit.NANOSECONDS);
                        long remainingHours = TimeUnit.HOURS.convert(remainingTime, TimeUnit.NANOSECONDS);
                        String progressLog = String.format( "Approximately %d %% of all walks done; estimated remaining time is %d minutes or %d hours", progress, remainingMinutes, remainingHours);
                        log.info(progressLog);
                    }
                    // ------

                    output.addResult(pathIds);
                } catch (Exception e) {
                    String errorMsg = "Error with node " + nodeId + ": " + e.getMessage();
                    System.out.println(errorMsg);
                    log.error(errorMsg);

                }
            });
        });

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Thread join timed out");
        }

        output.endInput();
    }

    private long estimateRemainingTime(long startNanoTime, long currentIndex, long size) {
        long elapsedTime = System.nanoTime() - startNanoTime;
        double timeForOne = elapsedTime / currentIndex;
        long remainingElements = size - currentIndex;
        return remainingElements * (long) timeForOne;
    }

    private RandomWalkProc.RandomPath convertIdsToPath(long[][] pathIds){
//        RandomWalkProc.RandomPath path = new RandomWalkProc.RandomPath(pathIds[1].length);
        int pathLength = pathIds[0].length;
        RandomWalkProc.RandomPath path = new RandomWalkProc.RandomPath(pathLength);
        try (Transaction tx = db.beginTx()) {
            // path[0] contains Ids of nodes, path[1] contains Ids of relationships
            for(int i = 0; i < pathLength - 1; i++){
                Node node = db.getNodeById(pathIds[0][i]);
                Node nextNode = db.getNodeById(pathIds[0][i+1]);
//                Relationship relationship = db.getRelationshipById(pathIds[1][i]);


                path.addNode(node);
                // TODO find a way to get relationships directly and remove this hack
                Relationship relationship = findRelationshipForNodes(node, nextNode);
                path.addRelationship(relationship);
            }
            // Add last node (that doesn't have a relationship)
            Node node = db.getNodeById(pathIds[0][pathIds[0].length - 1]);
            path.addNode(node);
            tx.success();
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
        return path;
    }

    private Relationship findRelationshipForNodes(Node a, Node b){
        for (Relationship relationship:a.getRelationships()){
            if(relationship.getOtherNode(a).equals(b)){
                return relationship;
            }
        }
        return null;
    }

    private long[][] doRandomWalk(int startNodeId, int steps) {
        long[] nodeIds = new long[(int) steps + 1];
        int nodeId = startNodeId;
        nodeIds[0] = getOriginalId(nodeId);
        for(int i = 1; i <= steps; i++){
            nodeId = getRandomNeighbour(nodeId);
            if (nodeId == -1) {
                nodeIds = new long[1];
                nodeIds[0] = getOriginalId(startNodeId);
                // End walk when there is no way out and return empty result
                break;
            }
            nodeIds[i] = getOriginalId(nodeId);
        }
        long[][] pack = {nodeIds, {}};

        return pack;
    }

    private int getRandomNeighbour(int nodeId) {
        int degree = degrees.degree(nodeId, Direction.OUTGOING);
        if(degree == 0){
            return -1;
        }
        int randomEdgeIndex= random.nextInt(degree);
        int neighbourId = arrayGraphInterface.getOutgoingNodes(nodeId)[randomEdgeIndex];

        return neighbourId;
    }

    private long getOriginalId(int nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    private int getMappedId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    public static abstract class AbstractWalkOutput {
        public AbstractWalkOutput(){
        }

        public abstract void endInput();

        public abstract void addResult(long[][] result);

        public Stream<RandomWalkProc.RandomWalkResult> getStream() {
            return Stream.empty();
        }

        public abstract int numberOfResults();
    }

    public static class RandomWalkNodeDirectFileOutput extends AbstractWalkOutput {
        private PrintStream output;
        private int count = 0;

        public RandomWalkNodeDirectFileOutput(String filePath) throws IOException {
            super();
            this.output = new PrintStream(new FileOutputStream(filePath));
        }

        public void endInput(){
            this.output.close();
        }

        public synchronized void addResult(long[][] result) {
            count++;
            this.output.println(arrayToString(result[0]));
        }

        private String arrayToString(long[] array){
            String str = String.valueOf(array[0]);
            for(int i = 1; i < array.length; i++){
                long l = array[i];
                str += " " + String.valueOf(l);
            }
            return str;
        }

        public int numberOfResults(){
            return count;
        }
    }

    public static class RandomWalkDatabaseOutput extends AbstractWalkOutput {
        public ArrayList<long[][]> pathIds = new ArrayList<>();
        private RandomWalk instance;
        private Iterator<long[][]> pathIterator;

        public RandomWalkDatabaseOutput(RandomWalk instance) {
            super();
            this.instance = instance;
        }

        public void endInput(){
            pathIterator = pathIds.iterator();
        }

        public synchronized void addResult(long[][] result) {
            pathIds.add(result);
        }

        public Stream<RandomWalkProc.RandomWalkResult> getStream() {
            return Stream.generate(
                    () -> new RandomWalkProc.RandomWalkResult(instance.convertIdsToPath(pathIterator.next()))).limit(pathIds.size());
        }

        public int numberOfResults(){
            return pathIds.size();
        }
    }
}
