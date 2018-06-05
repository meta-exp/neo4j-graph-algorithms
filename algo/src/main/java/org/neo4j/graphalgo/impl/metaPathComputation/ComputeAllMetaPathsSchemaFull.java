package org.neo4j.graphalgo.impl.metaPathComputation;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ComputeAllMetaPathsSchemaFull extends MetaPathComputation {

    private int metaPathLength;
    private PrintStream debugOut;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private ArrayList<HashSet<Pair>> schema;
    private HashMap<Integer, Integer> reversedLabelDictionary;
    private PrintStream out;
    private long startTime;
    private long endTime;

    public ComputeAllMetaPathsSchemaFull(int metaPathLength, ArrayList<HashSet<Pair>> schema, HashMap<Integer, Integer> reversedLabelDictionary) throws Exception {
        this.metaPathLength = metaPathLength;
        this.schema = schema;
        this.reversedLabelDictionary = reversedLabelDictionary;

        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Full_Debug.txt"));
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Full.txt"));//ends up in root/tests //or in dockerhome
    }

    public Result compute() throws IOException {
        debugOut.println("START SCHEMA_FULL");

        startTime = System.nanoTime();
        List<Runnable> threads = startThreads();
        mergeThreads(threads);
        endTime = System.nanoTime();
        debugOut.println("FINISH SCHEMA_FULL after " + (endTime - startTime) / 1000000 + " milliseconds");

        for (String mp : duplicateFreeMetaPaths) {
            out.println(mp);
        }

        return new Result(duplicateFreeMetaPaths);
    }

    private void mergeThreads(List<Runnable> threads) {
        for (Runnable thread : threads) {
            duplicateFreeMetaPaths.addAll(((ComputeMetaPathFromNodeIdThread) thread).getDuplicateFreeMetaPaths());
        }
    }

    private List<Runnable> startThreads() {
        int processorCount = Runtime.getRuntime().availableProcessors();
        List<Runnable> threads = new ArrayList<>();
        debugOut.println("ProcessorCount: " + processorCount);
        debugOut.println("schema-size: " + schema.size());
        debugOut.println("reverseLabelDictionary-size: " + reversedLabelDictionary.size());
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);

        for (int i = 0; i < schema.size(); i++) {
            Runnable worker = new ComputeMetaPathFromNodeIdThread(i, metaPathLength);
            threads.add(worker);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        return threads;
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        for (int label : currentMetaPath) {
            newMetaPath.add(label);
        }

        return newMetaPath;
    }

    private class ComputeMetaPathFromNodeIdThread implements Runnable {
        private int nodeId;
        private int metaPathLength;
        private HashSet<String> duplicateFreeMetaPathsOfThread;

        ComputeMetaPathFromNodeIdThread(int nodeId, int metaPathLength) {
            this.nodeId = nodeId;
            this.metaPathLength = metaPathLength;
            this.duplicateFreeMetaPathsOfThread = new HashSet<>();
        }

        public void computeMetaPathFromNodeLabel(int nodeID, int metaPathLength) {
            ArrayList<Integer> initialMetaPath = new ArrayList<>();
            initialMetaPath.add(reversedLabelDictionary.get(nodeID)); //because nodeID is already a type of nodes in the real graph//convert to heavyGraph nodeType

            addAndLogMetaPath(initialMetaPath);
            computeMetaPathFromNodeLabel(initialMetaPath, nodeID, metaPathLength - 1);
        }

        private void computeMetaPathFromNodeLabel(ArrayList<Integer> currentMetaPath, int currentInstance, int metaPathLength) {
            if (metaPathLength <= 0) return;

            HashSet<Pair> neighbourNodesAndEdges = schema.get(currentInstance);
            for (Pair neighbourNode_edge : neighbourNodesAndEdges) {
                ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                int nodeID = neighbourNode_edge.first();
                int edgeID = neighbourNode_edge.second();
                newMetaPath.add(edgeID);
                newMetaPath.add(reversedLabelDictionary.get(nodeID));

                addAndLogMetaPath(newMetaPath);
                computeMetaPathFromNodeLabel(newMetaPath, nodeID, metaPathLength - 1);
            }
        }

        private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
            String joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
            duplicateFreeMetaPathsOfThread.add(joinedMetaPath);
        }

        public void run() {
            computeMetaPathFromNodeLabel(nodeId, metaPathLength);
        }

        public HashSet<String> getDuplicateFreeMetaPaths() {
            return duplicateFreeMetaPathsOfThread;
        }
    }

    //TODO -------------------------------------------------------------------

    @Override
    public ComputeAllMetaPathsSchemaFull me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsSchemaFull release() {
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