package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.NodeWalkerProc;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NodeWalker extends AbstractWalkAlgorithm {

    private static Random random = new Random();

    private AbstractNextNodeStrategy nextNodeStrategy;

    public NodeWalker(HeavyGraph graph, Log log, AbstractNextNodeStrategy nextNodeStrategy){
        super(graph, log);
        this.nextNodeStrategy = nextNodeStrategy;
    }

    public Stream<NodeWalkerProc.WalkResult> walkFromNode(AbstractWalkOutput output, long nodeId, long steps, long walks) {
        Stream<Integer> stream = Stream.generate(() -> getMappedId(nodeId)).limit(walks);

        startWalks(output, stream, walks, steps);

        return output.getStream();
    }

    public Stream<NodeWalkerProc.WalkResult> walkFromNodeType(AbstractWalkOutput output, long steps, long walks, String type) {
        Stream<Integer> startNodeIdStream;
        if (type.isEmpty()) {
            startNodeIdStream = randomNodesFromAllNodes((int) walks);
        } else {
            startNodeIdStream = Stream.empty();
//            startNodes = randomNodesFromType(type, walks);
        }

        startWalks(output, startNodeIdStream, walks, steps);

        return output.getStream();
    }

    public Stream<NodeWalkerProc.WalkResult> walkFromAllNodes(AbstractWalkOutput output, long steps, long walks) {
        // TODO: find out why sometimes not all nodes are visited.

        long nodeCount = graph.nodeCount();
        long totalWalks = nodeCount * walks;

        Stream<Integer> stream = Stream.empty();
        for (int i = 0; i < walks; i++) {
            Stream<Integer> nodeIdStream = IntStream.range(0, (int) graph.nodeCount()).boxed();

            stream = Stream.concat(stream, nodeIdStream);
        }

        startWalks(output, stream, totalWalks, steps);

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


    private void startWalks(AbstractWalkOutput output, Stream<Integer> nodeStream, long numberOfElements, long steps) {
        int cores = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores * 4);
        executor.setCorePoolSize(cores * 4);

        long startTime = System.nanoTime();

        nodeStream.forEach((nodeId)->{
            executor.execute(() -> {
                try {
                    long[][] pathIds = doWalk(nodeId, (int) steps, nextNodeStrategy);
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

    private long[][] doWalk(int startNodeId, int steps, AbstractNextNodeStrategy nextNodeStrategy) {
        long[] nodeIds = new long[(int) steps + 1];
        int currentNodeId = startNodeId;
        int previousNodeId = currentNodeId;
        nodeIds[0] = getOriginalId(currentNodeId);
        for(int i = 1; i <= steps; i++){
            int nextNodeId = nextNodeStrategy.getNextNode(currentNodeId, previousNodeId);
            previousNodeId = currentNodeId;
            currentNodeId = nextNodeId;

            if (currentNodeId == -1) {
                nodeIds = new long[1];
                nodeIds[0] = getOriginalId(startNodeId);
                // End walk when there is no way out and return empty result
                break;
            }
            nodeIds[i] = getOriginalId(currentNodeId);
        }
        long[][] pack = {nodeIds, {}};

        return pack;
    }

    public static abstract class AbstractNextNodeStrategy {

        private ArrayGraphInterface arrayGraphInterface;
        private Degrees degrees;

        public AbstractNextNodeStrategy(ArrayGraphInterface arrayGraphInterface, Degrees degrees){
            this.arrayGraphInterface = arrayGraphInterface;
            this.degrees = degrees;
        }

        public abstract int getNextNode(int currentNodeId, int previousNodeId);

    }

    public static class RandomNextNodeStrategy extends AbstractNextNodeStrategy{
        private static Random random = new Random();

        public RandomNextNodeStrategy(ArrayGraphInterface arrayGraphInterface, Degrees degrees){
            super(arrayGraphInterface, degrees);
        }

        public int getNextNode(int currentNodeId, int previousNodeId){
            int degree = super.degrees.degree(currentNodeId, Direction.BOTH);
            if(degree == 0){
                return -1;
            }
            int randomEdgeIndex= random.nextInt(degree);
            int neighbourId = super.arrayGraphInterface.getAdjacentNodes(currentNodeId)[randomEdgeIndex];

            return neighbourId;
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
            int neighbourId = super.arrayGraphInterface.getAdjacentNodes(currentNodeId)[neighbourIndex];

            return neighbourId;
        }

        private float[] buildProbabilityDistribution(int currentNodeId, int previousNodeId,
                                                   double returnParam, double inOutParam){
            int[] neighbours = super.arrayGraphInterface.getAdjacentNodes(currentNodeId);
            int[] previousNeighbours = super.arrayGraphInterface.getAdjacentNodes(previousNodeId);
            List<Integer> prevList = IntStream.of(previousNeighbours).boxed().collect(Collectors.toList());

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
                array[i] = array[i] / sum;
            }
            return array;
        }

        private int pickIndexFromDistribution(float[] distribution){
            double p = random.nextDouble();
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
