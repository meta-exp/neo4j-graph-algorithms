package org.neo4j.graphalgo.impl.walking;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NodeWalker extends AbstractWalkAlgorithm {

    private static Random random = ThreadLocalRandom.current();

    private AbstractNextNodeStrategy nextNodeStrategy;

    public NodeWalker(HeavyGraph graph, Log log, AbstractNextNodeStrategy nextNodeStrategy){
        super(graph, log);
        this.nextNodeStrategy = nextNodeStrategy;
    }

    public Stream<WalkResult> walkFromNode(AbstractWalkOutput output, long nodeId, long steps, long walks) {

        IntStream stream = IntStream.generate(() -> getMappedId(nodeId)).limit(walks);

        startWalks(output, stream, walks, steps);

        return output.getStream();
    }

    public Stream<WalkResult> walkFromNodeType(AbstractWalkOutput output, long steps, long walks, String type) {
        IntStream startNodeIdStream;
        if (type.isEmpty()) {
            startNodeIdStream = randomNodesFromAllNodes((int) walks);
        } else {
            startNodeIdStream = IntStream.empty();
//            startNodes = randomNodesFromType(type, walks);
        }

        startWalks(output, startNodeIdStream, walks, steps);

        return output.getStream();
    }

    public Stream<WalkResult> walkFromAllNodes(AbstractWalkOutput output, long steps, long walks) {
        // TODO: find out why sometimes not all nodes are visited.

        long nodeCount = graph.nodeCount();
        long totalWalks = nodeCount * walks;

        IntStream stream = IntStream.empty();
        for (int i = 0; i < walks; i++) {
            IntStream nodeIdStream = IntStream.range(0, (int) graph.nodeCount());

            stream = IntStream.concat(stream, nodeIdStream);
        }

        startWalks(output, stream, totalWalks, steps);

        return output.getStream();
    }


    private IntStream randomNodesFromAllNodes(int walks) {
        int nodeCount = (int) graph.nodeCount(); // data type of mapped id is int, so casting shouldn't be a problem
        return IntStream.generate(() -> random.nextInt(nodeCount)).limit(walks);
    }

//TODO
//    private List<Node> randomNodesFromType(String type, long walks) {
//        long nodeCount = getNodeCount();
//
//        return randomNodesFromIterator(db.findNodes(Label.label(type)), listRandomNumbers((int) nodeCount, walks));
//    }


    private void startWalks(AbstractWalkOutput output, IntStream nodeStream, long numberOfElements, long steps) {
        BoundedExecutor executor = getBoundedExecutor();

        /*
        ExecutorService pool = null;

        List<Future<Integer>> futures;
        futures.add(pool.submit(() -> 42));
        futures.stream().map(Future::get).flatMap()
        */

        long startTime = System.nanoTime();

        nodeStream.forEach((nodeId)->{
            try {
                executor.submitTask(() -> {
                    startSingleWalk(output, nodeId, (int) steps, numberOfElements, startTime);
                });
            } catch (InterruptedException e) {
                log.error("Thread waiting interrupted");
            }

        });

        executor.getExecutor().shutdown();
        try {
            executor.getExecutor().awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Thread join timed out");
        }

        output.endInput();
    }

    private void startSingleWalk(AbstractWalkOutput output, int nodeId, int steps, long numberOfElements, long startTime) {
        try {
            long[] pathIds = doWalk(nodeId, steps, nextNodeStrategy);
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
    }

    private long estimateRemainingTime(long startNanoTime, long currentIndex, long size) {
        long elapsedTime = System.nanoTime() - startNanoTime;
        double timeForOne = elapsedTime / currentIndex;
        long remainingElements = size - currentIndex;
        return remainingElements * (long) timeForOne;
    }

    private long[] doWalk(int startNodeId, int steps, AbstractNextNodeStrategy nextNodeStrategy) {
        long[] nodeIds = new long[steps + 1];
        int currentNodeId = startNodeId;
        int previousNodeId = currentNodeId;
        nodeIds[0] = getOriginalId(currentNodeId);
        for(int i = 1; i <= steps; i++){
            int nextNodeId = nextNodeStrategy.getNextNode(currentNodeId, previousNodeId);
            previousNodeId = currentNodeId;
            currentNodeId = nextNodeId;

            if (currentNodeId == -1) {
                // End walk when there is no way out and return empty result
                return Arrays.copyOf(nodeIds,1);
            }
            nodeIds[i] = getOriginalId(currentNodeId);
        }

        return nodeIds;
    }

    public static abstract class AbstractNextNodeStrategy {

        private ArrayGraphInterface arrayGraphInterface;
        private Degrees degrees;

        public AbstractNextNodeStrategy(ArrayGraphInterface arrayGraphInterface, Degrees degrees){
            this.arrayGraphInterface = arrayGraphInterface;
            this.degrees = degrees;
        }

        public abstract int getNextNode(int currentNodeId, int previousNodeId);

        protected Random getRandom(){
            return ThreadLocalRandom.current();
        }

    }

    public static class RandomNextNodeStrategy extends AbstractNextNodeStrategy{

        public RandomNextNodeStrategy(ArrayGraphInterface arrayGraphInterface, Degrees degrees){
            super(arrayGraphInterface, degrees);
        }

        public int getNextNode(int currentNodeId, int previousNodeId){
            int degree = super.degrees.degree(currentNodeId, Direction.BOTH);
            if(degree == 0){
                return -1;
            }
            int randomEdgeIndex = getRandom().nextInt(degree);

            return super.arrayGraphInterface.getRelationship(currentNodeId,randomEdgeIndex);
        }
    }

    public static class Node2VecStrategy extends AbstractNextNodeStrategy{
        private static Random random = new Random();
        private double returnParam, inOutParam;


        public Node2VecStrategy(ArrayGraphInterface arrayGraphInterface, Degrees degrees,
                                double returnParam, double inOutParam){
            super(arrayGraphInterface, degrees);
            this.returnParam = returnParam;
            this.inOutParam = inOutParam;
        }

        public int getNextNode(int currentNodeId, int previousNodeId){
            int degree = super.degrees.degree(currentNodeId, Direction.BOTH);
            if(degree == 0){
                return -1;
            }

            float[] distribution = buildProbabilityDistribution(currentNodeId, previousNodeId, returnParam, inOutParam);
            int neighbourIndex = pickIndexFromDistribution(distribution);

            return super.arrayGraphInterface.getRelationship(currentNodeId,neighbourIndex);
        }

        private float[] buildProbabilityDistribution(int currentNodeId, int previousNodeId,
                                                   double returnParam, double inOutParam){
            int[] neighbours = super.arrayGraphInterface.getAdjacentNodes(currentNodeId);
            int[] previousNeighbours = super.arrayGraphInterface.getAdjacentNodes(previousNodeId);
            IntSet prevList = IntHashSet.from(previousNeighbours);

            float[] probabilities = new float[neighbours.length];

            float probSum = 0;

            for(int i = 0; i < neighbours.length; i++){ // Calculate probabilities for all adjacent nodes
                int neighbourId = neighbours[i];
                float probability;

                if(neighbourId == previousNodeId){
                    // node is previous node
                    probability = 1f / ((float) returnParam);
                } else if (prevList.contains(neighbourId)){
                    // node is also adjacent to previous node --> distance to previous node is 1
                    probability = 1f;
                } else {
                    // node is not adjacent to previous node --> distance to previous node is 2
                    probability = 1f / ((float) inOutParam);
                }
                probabilities[i] = probability;
                probSum += probability;
            }
            probabilities = normalizeDistribution(probabilities, probSum);
            return probabilities;
        }

        private float[] normalizeDistribution(float[] array, float sum){
            for(int i = 0; i < array.length; i++){
                array[i] /=  sum;
            }
            return array;
        }

        private int pickIndexFromDistribution(float[] distribution){
            double p = getRandom().nextDouble();
            double cumulativeProbability = 0.0;
            for(int i = 0; i < distribution.length; i++){
                cumulativeProbability += distribution[i];
                if (p <= cumulativeProbability) {
                    return i;
                }
            }
            return distribution.length - 1;
        }
    }
}
