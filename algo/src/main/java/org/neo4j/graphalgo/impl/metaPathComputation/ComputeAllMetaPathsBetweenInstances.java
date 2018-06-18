package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ComputeAllMetaPathsBetweenInstances extends MetaPathComputation {

    private int metaPathLength;
    private PrintStream debugOut;
    private HeavyGraph graph;

    public ComputeAllMetaPathsBetweenInstances(HeavyGraph graph, int metaPathLength) throws Exception {
        this.metaPathLength = metaPathLength;
        this.graph = graph;

        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Full_Debug.txt"));
    }

    public Result compute() throws IOException {
        debugOut.println("START BETWEEN_INSTANCES");

        long startTime = System.nanoTime();
        startThreads();
        long endTime = System.nanoTime();
        debugOut.println("FINISH BETWEEN_INSTANCES after " + (endTime - startTime) / 1000000 + " milliseconds");

        return new Result(new HashSet<>());
    }

    private void startThreads() {
        int processorCount = Runtime.getRuntime().availableProcessors();
        debugOut.println("ProcessorCount: " + processorCount);
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);


        graph.forEachNode(node -> {
            int[] adjacent_nodes = graph.getAdjacentNodes(node);
            for(int adjacent_node: adjacent_nodes){
                Runnable worker = new ComputeMetaPathFromNodeIdThread(node, adjacent_node, metaPathLength);
                executor.execute(worker);
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

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        for (int label : currentMetaPath) {
            newMetaPath.add(label);
        }

        return newMetaPath;
    }

    private class ComputeMetaPathFromNodeIdThread implements Runnable {
        private int start_nodeId;
        private int end_nodeID;
        private int metaPathLength;
        private HashSet<String> duplicateFreeMetaPathsOfThread;

        ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength) {
            this.start_nodeId = start_nodeId;
            this.end_nodeID = end_nodeID;
            this.metaPathLength = metaPathLength;
            this.duplicateFreeMetaPathsOfThread = new HashSet<>();
        }

        public void computeMetaPathFromNodeID(int start_nodeId, int end_nodeID, int metaPathLength) {
            ArrayList<Integer> initialMetaPath = new ArrayList<>();
            initialMetaPath.add(graph.getLabel(start_nodeId));

            computeMetaPathFromNodeID(initialMetaPath, start_nodeId, end_nodeID, metaPathLength - 1);
            try {
                PrintStream out = new PrintStream(new FileOutputStream("between_instances/MetaPaths_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph.toOriginalNodeId(end_nodeID) + ".txt"));
                for (String mp : duplicateFreeMetaPathsOfThread) {
                    out.println(mp);
                }
                out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void computeMetaPathFromNodeID(ArrayList<Integer> currentMetaPath, int currentInstance, int end_nodeID, int metaPathLength) {
            if (metaPathLength <= 0) return;

            if (currentInstance == end_nodeID) {
                addAndLogMetaPath(currentMetaPath);
                return;
            }

            for(int node: graph.getAdjacentNodes(currentInstance)) {
                ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                newMetaPath.add(graph.getEdgeLabel(currentInstance, node));
                newMetaPath.add(graph.getLabel(currentInstance));
                computeMetaPathFromNodeID(newMetaPath, node, end_nodeID, metaPathLength - 1);
            }
        }

        private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
            String joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
            duplicateFreeMetaPathsOfThread.add(joinedMetaPath);
        }

        public void run() {
            computeMetaPathFromNodeID(start_nodeId, end_nodeID, metaPathLength);
        }

        public HashSet<String> getDuplicateFreeMetaPaths() {
            return duplicateFreeMetaPathsOfThread;
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