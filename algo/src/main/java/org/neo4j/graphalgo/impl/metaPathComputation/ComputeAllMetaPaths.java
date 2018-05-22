package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.Pair;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Float.max;

public class ComputeAllMetaPaths extends MetaPathComputation {

    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private IdMap mapping;
    private int metaPathLength;
    private ArrayList<HashSet<Integer>> initialInstances;
    private int currentLabelId = 0;
    private ArrayList<String> duplicateFreeMetaPaths;
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<Pair, Integer> labelDictionary;

    public ComputeAllMetaPaths(HeavyGraph graph, IdMapping idMapping,
                               ArrayGraphInterface arrayGraphInterface,
                               Degrees degrees, int metaPathLength) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.metaPathLength = metaPathLength;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size() * arrayGraphInterface.getAllEdgeLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);
        this.labelDictionary = new HashMap<>();
        this.duplicateFreeMetaPaths = new ArrayList<>();
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        ArrayList<String> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        System.out.println("calculation took: " + String.valueOf(endTime - startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime - startTime));
        debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    public ArrayList<String> computeAllMetaPaths() {

        initializeLabelDictAndInitialInstances();
        computeMetaPathsFromAllNodeLabels();

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        HashMap<Integer, Integer> labelCountDict = new HashMap<>();

        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
                assignIdToNodeLabel(edgeLabel, nodeLabel);
            }
        }

        graph.forEachNode(node -> initializeNode(node, labelCountDict));
        for(int label : arrayGraphInterface.getAllLabels())
        {
            createMetaPathWithLengthOne(label, labelCountDict.get(label));
        }
    }

    private boolean initializeNode(int node, HashMap<Integer, Integer> labelCountDict) {
        int nodeLabel = arrayGraphInterface.getLabel(node);
        int edgeLabel = arrayGraphInterface.getAllEdgeLabels().iterator().next();
        labelCountDict.put(nodeLabel, 1 + (labelCountDict.get(nodeLabel) == null ? 0 : labelCountDict.get(nodeLabel)));

        Integer nodeLabelId = labelDictionary.get(new Pair(edgeLabel, nodeLabel));
        initialInstances.get(nodeLabelId).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int edgeLabel, int nodeLabel) {
        labelDictionary.put(new Pair(edgeLabel, nodeLabel), currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

    private boolean createMetaPathWithLengthOne(int nodeLabel, int instanceCountSum) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath, instanceCountSum);
        return true;
    }

    private void computeMetaPathsFromAllNodeLabels() {
        int processorCount = Runtime.getRuntime().availableProcessors();
        debugOut.println("ProcessorCount: " + processorCount);
        ExecutorService executor = Executors.newFixedThreadPool(processorCount);
        int i = 0;
        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            Runnable worker = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, nodeLabel, metaPathLength);
            executor.execute(worker);
            i++;
        }
        executor.shutdown();
        while(!executor.isTerminated()){}
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> currentMetaPath, HashMap<Integer, Integer> currentInstances, int metaPathLength) {
        if (metaPathLength <= 0) {
            return;
        }

        ArrayList<HashMap<Integer, Integer>> nextInstances = calculateNextInstances(currentInstances);

        for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
            for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
                int instancesArrayIndex = labelDictionary.get(new Pair(edgeLabel, nodeLabel));
                HashMap<Integer, Integer> nextInstancesForLabel = nextInstances.get(instancesArrayIndex);
                if (!nextInstancesForLabel.isEmpty()) {
                    nextInstances.set(instancesArrayIndex, null);

                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    newMetaPath.add(edgeLabel);
                    newMetaPath.add(nodeLabel);

                    long instanceCountSum = 0;
                    for (int count : nextInstancesForLabel.values()) {//refactor to stream?
                        instanceCountSum += count;
                    }

                    addAndLogMetaPath(newMetaPath, instanceCountSum);
                    computeMetaPathFromNodeLabel(newMetaPath, nextInstancesForLabel, metaPathLength - 1);
                }
            }
        }
    }


    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum) {
            String joinedMetaPath = addMetaPath(newMetaPath, instanceCountSum);
            printMetaPathAndLog(joinedMetaPath);

    }

    private ArrayList<HashMap<Integer, Integer>> allocateNextInstances() {
        int nextInstancesSize = arrayGraphInterface.getAllLabels().size() * arrayGraphInterface.getAllEdgeLabels().size();
        ArrayList<HashMap<Integer, Integer>> nextInstances = new ArrayList<>(nextInstancesSize);

        for (int i = 0; i < nextInstancesSize; i++) {
            nextInstances.add(new HashMap<>());
        }

        return nextInstances;
    }

    private ArrayList<HashMap<Integer, Integer>> calculateNextInstances(HashMap<Integer, Integer> currentInstances) {
        ArrayList<HashMap<Integer, Integer>> nextInstances = allocateNextInstances();
        for (int instance : currentInstances.keySet()) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) {
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int edgeLabel = arrayGraphInterface.getEdgeLabel(instance, nodeId);
                int labelID = labelDictionary.get(new Pair(edgeLabel, label));

                boolean incrementMissing = nextInstances.get(labelID).get(nodeId) == null;
                int oldCount = currentInstances.get(instance);
                int count = oldCount + (incrementMissing ? 0 : nextInstances.get(labelID).get(nodeId));

                nextInstances.get(labelID).put(nodeId, count); // add the node to the corresponding instances array
            }
        }
        return nextInstances;
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        newMetaPath.addAll(currentMetaPath);

        return newMetaPath;
    }

    private String addMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum) {
        String joinedMetaPath;
        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        joinedMetaPath += "\t" + instanceCountSum;
        duplicateFreeMetaPaths.add(joinedMetaPath);

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
        printCount++;
        if (printCount % max(((int) estimatedCount / 50), 1) == 0 && estimatedCount != 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100 * printCount / estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    public void computeMetaPathFromNodeLabel(int startNodeLabel, int metaPathLength) {
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(startNodeLabel);
        HashMap<Integer, Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
    }

    private HashMap<Integer, Integer> initInstancesRow(int startNodeLabel) {
        int startEdgeLabel = arrayGraphInterface.getAllEdgeLabels().iterator().next();
        int startNodeLabelId = labelDictionary.get(new Pair(startEdgeLabel, startNodeLabel));
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        HashMap<Integer, Integer> dictRow = new HashMap<>();
        for (int instance : row) {
            dictRow.put(instance, 1);
        }
        return dictRow;
    }

    //TODO------------------------------------------------------------------------------------------------------------------


    @Override
    public ComputeAllMetaPaths me() {
        return this;
    }

    @Override
    public ComputeAllMetaPaths release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        ArrayList<String> finalMetaPaths;

        public Result(ArrayList<String> finalMetaPaths) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<String> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }
}
