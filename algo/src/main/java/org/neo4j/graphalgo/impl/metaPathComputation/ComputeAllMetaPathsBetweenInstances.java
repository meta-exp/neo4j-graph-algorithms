package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.metaPathComputationProcs.ComputeMetaPathFromNodeIdThread;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ComputeAllMetaPathsBetweenInstances extends MetaPathComputation {

    private int         metaPathLength;
    private HeavyGraph  graph;
    public  Log         log;
    private float nodePairSkipProbability = 0;
    private float edgeSkipProbability = 0;

    public ComputeAllMetaPathsBetweenInstances(HeavyGraph graph, int metaPathLength,  Log log){
        this.metaPathLength = metaPathLength;
        this.graph = graph;
        this.log = log;
    }

    public ComputeAllMetaPathsBetweenInstances(HeavyGraph graph, int metaPathLength,  Log log, float nodePairSkipProbability, float edgeSkipProbability){
        this.metaPathLength = metaPathLength;
        this.graph = graph;
        this.log = log;
        this.nodePairSkipProbability = nodePairSkipProbability;
        this.edgeSkipProbability = edgeSkipProbability;
    }

    public Result compute() {
        log.info("START BETWEEN_INSTANCES");

        long startTime = System.nanoTime();
        startThreads();
        long endTime = System.nanoTime();
        log.info("FINISH BETWEEN_INSTANCES after " + (endTime - startTime) / 1000000 + " milliseconds");

        return new Result(new HashSet<>());
    }

    private void startThreads() {
        int processorCount = Runtime.getRuntime().availableProcessors();
        log.info("ProcessorCount: " + processorCount);
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);

        Random random = new Random(42);

        graph.forEachNode(node -> {
            int[] adjacent_nodes = graph.getAdjacentNodes(node);
            for(int adjacent_node: adjacent_nodes){
                if (random.nextFloat() > this.nodePairSkipProbability) {
                    Runnable worker = new ComputeMetaPathFromNodeIdThread(node, adjacent_node, metaPathLength, this.edgeSkipProbability, graph, log);
                    executor.execute(worker);
                }
            }
            return true;
        });
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //TODO -------------------------------------------------------------------

    @Override
    public ComputeAllMetaPathsBetweenInstances me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsBetweenInstances release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        HashSet<String> finalMetaPaths;

        public Result(HashSet<String> finalMetaPaths) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashSet<String> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }
}