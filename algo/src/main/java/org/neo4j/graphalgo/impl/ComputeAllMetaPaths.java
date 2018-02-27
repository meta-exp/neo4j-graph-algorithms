package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.HandyStuff;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ComputeAllMetaPaths extends Algorithm<ComputeAllMetaPaths> {

    private HeavyGraph graph;
    private HandyStuff handyStuff;
    private Degrees degrees;
    private IdMap mapping;
    private ArrayList<ArrayList<String>> metaPaths;
    private ArrayList<Integer> metaPathsWeights;
    private int metaPathLength;
    private HashMap<String, Byte> labelDictionary;
    private ArrayList<ArrayList<Integer>> initialInstances;
    private int max_instance_count; //TODO: not needed anymore?
    private byte currentLabelId = 0;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    //private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               HandyStuff handyStuff,
                               Degrees degrees, int metaPathLength, int max_label_count, int max_instance_count) throws IOException {
        this.graph = graph;
        this.handyStuff = handyStuff;
        this.degrees = degrees;
        this.metaPaths = new ArrayList<>();
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.max_instance_count = max_instance_count;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < max_label_count; i++) {
            this.initialInstances.add(new ArrayList<>());
        }
        this.labelDictionary = new HashMap<>();
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        //this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(max_label_count, metaPathLength + 1);

    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds) {
        for (long l : incomingIds) {
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {
        //debugOut.println("started computation");
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetapaths();
        long endTime = System.nanoTime();

        //System.out.println("calculation took: " + String.valueOf(endTime-startTime));
        //debugOut.println("actual amount of metaPaths: " + printCount);
        //debugOut.println("total time past: " + (endTime-startTime));

        startTime = System.nanoTime();
        //System.out.println("Writing to disk took: " + String.valueOf(startTime-endTime));

        //debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    public HashSet<String> computeAllMetapaths() {
        //debugOut.println("starting initializeLabelDictAndInitialInstances");
        initializeLabelDictAndInitialInstances();
        //debugOut.println("finished initializeLabelDictAndInitialInstances");
        //debugOut.println("starting computeMetaPathsFromAllNodeLabels");
        computeMetaPathsFromAllNodeLabels();
        //debugOut.println("finished computeMetaPathsFromAllNodeLabels");

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        graph.forEachNode(node -> initializeNode(node));
        //debugOut.println("labelDictionary size: " + labelDictionary.size());
    }

    private boolean initializeNode(int node) {
        //debugOut.println("initializing node: " + node);
        String nodeLabel = handyStuff.getLabel(node);
        Byte nodeLabelId = labelDictionary.get(nodeLabel);
        //debugOut.println("looked up nodeLabel and labelId");

        if (nodeLabelId == null) {
            nodeLabelId = assignIdToNodeLabel(nodeLabel);
            //debugOut.println("added label to labelDict and got new id");
            createMetaPathWithLengthOne(nodeLabel);
        }

        //debugOut.println("metaPath of lenght 1 handeled");

        initialInstances.get(nodeLabelId).add(node);
        //debugOut.println("finished adding node: " + node);
        return true;
    }

    private void createMetaPathWithLengthOne(String nodeLabel) {
        ArrayList<String> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        int oldSize = duplicateFreeMetaPaths.size();
        String joinedMetaPath = addMetaPath(metaPath);
        int newSize = duplicateFreeMetaPaths.size();
        //debugOut.println("tried to add metaPath of length 1");
        if (newSize > oldSize) {
            printMetaPathAndLog(joinedMetaPath);
            //debugOut.println("added a not seen before metaPath");
        }
    }

    private byte assignIdToNodeLabel(String nodeLabel) {
        labelDictionary.put(nodeLabel, currentLabelId);
        currentLabelId++;
        return (byte) (currentLabelId - 1);
    }

    private void computeMetaPathsFromAllNodeLabels() {
        for (String nodeLabel : handyStuff.getAllLabels()) {
            //debugOut.println("start computation for initial nodelabel: " + nodeLabel);
            computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
            //debugOut.println("finished computation for initial nodeLabel: " + nodeLabel);
        }
    }

    private void computeMetaPathFromNodeLabel(ArrayList<String> currentMetaPath, int[] currentInstances, int metaPathLength) {
        if (metaPathLength == 0) {
            //debugOut.println("aborting recursion");
            return;
        }
        ArrayList<ArrayList<Integer>> nextInstances = allocateNextInstances();
        nextInstances = fillNextInstances(currentInstances, nextInstances);
        for (ArrayList<Integer> nextInstancesForLabel : nextInstances) {
            if (!nextInstancesForLabel.isEmpty()) {
                ArrayList<String> newMetaPath = copyMetaPath(currentMetaPath);
                String label = handyStuff.getLabel(nextInstancesForLabel.get(0)); //get(0) since all have the same label.
                newMetaPath.add(label);
                int oldSize = duplicateFreeMetaPaths.size();
                String joinedMetaPath = addMetaPath(newMetaPath);
                int newSize = duplicateFreeMetaPaths.size();
                if (newSize > oldSize)
                    printMetaPathAndLog(joinedMetaPath);
                int[] recursiveInstances = convertArrayListToIntArray(nextInstancesForLabel);
                nextInstances = null;
                nextInstancesForLabel = null;
                currentInstances = null;
                //debugOut.println("newSize: " + newSize);

                computeMetaPathFromNodeLabel(newMetaPath, recursiveInstances, metaPathLength-1);  //do somehow dp instead?
                //debugOut.println("finished recursion of length: " + (metaPathLength - 1));
            }
        }
    }

    private ArrayList<ArrayList<Integer>> allocateNextInstances() {
        ArrayList<ArrayList<Integer>> nextInstances = new ArrayList<>();
        for (int i = 0; i < labelDictionary.size(); i++) {
            nextInstances.add(new ArrayList<>());
        }
        //debugOut.println("allocated nextInstances");

        return nextInstances;
    }

    private ArrayList<ArrayList<Integer>> fillNextInstances(int[] currentInstances, ArrayList<ArrayList<Integer>> nextInstances) {
        //debugOut.println("started filling nextInstances");
        int i = 0;
        int k = 0;
        for (int instance : currentInstances) {
            i++;
            //debugOut.println("currentInstance: " + i);
            for (int nodeId : handyStuff.getAdjacentNodes(instance)) {
                k++;
                //debugOut.println("adjacentNode: " + k);
                Byte labelID = labelDictionary.get(handyStuff.getLabel(nodeId)); //get the id of the label of the node
                nextInstances.get(labelID).add(nodeId); // add the node to the corresponding instances array
            }
        }
        //debugOut.println("finished filling nextInstances");

        return nextInstances;
    }

    private ArrayList<String> copyMetaPath(ArrayList<String> currentMetaPath) {
        ArrayList<String> newMetaPath = new ArrayList<>();
        for (String label : currentMetaPath) {
            newMetaPath.add(label);
        }
        //debugOut.println("copied currentMetaPath");

        return newMetaPath;
    }

    private String addMetaPath(ArrayList<String> newMetaPath) {
        metaPaths.add(newMetaPath);
        String joinedMetaPath = String.join(" | ", newMetaPath );
        duplicateFreeMetaPaths.add(joinedMetaPath);
        //debugOut.println("tried adding new Metapath");

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        printCount++;
        if (printCount % ((int)estimatedCount/50) == 0) {
            out.println(joinedMetaPath);
            //debugOut.println("Meta-paths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    private int[] convertArrayListToIntArray(ArrayList<Integer> nextInstancesForLabel) {
        int[] recursiveInstances = new int[nextInstancesForLabel.size()]; //convert ArrayList<String> to  int[] array //maybe this ist not necessary anymore. just change param
        for (int j = 0; j < nextInstancesForLabel.size(); j++) {
            recursiveInstances[j] = nextInstancesForLabel.get(j);
        }
        //debugOut.println("converted arrayList to int-array");

        return recursiveInstances;
    }

    private void computeMetaPathFromNodeLabel(String startNodeLabel, int metaPathLength) {
        ArrayList<String> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(startNodeLabel);
        //debugOut.println("startet computing all metaPaths form label: " + startNodeLabel);
        int[] initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
        //debugOut.println("finished recursion for: " + startNodeLabel);
    }

    private int[] initInstancesRow(String startNodeLabel) {
        Byte labelID = labelDictionary.get(startNodeLabel);
        int[] initialInstancesRow = new int[initialInstances.get(labelID).size()];
        for (int i = 0; i < initialInstancesRow.length; i++) { // maybe not needed anymore
            initialInstancesRow[i] = initialInstances.get(labelID).get(i);
        }
        //debugOut.println("finished getting the instancesRow for recursion");
        return initialInstancesRow;
    }
//TODO------------------------------------------------------------------------------------------------------------------
    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new Result(new HashSet<>()));
    }

    @Override
    public ComputeAllMetaPaths me() { return this; }

    @Override
    public ComputeAllMetaPaths release() {
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

    public void weight (int index, int weight) throws Exception {
        if (weight <= 0 || weight > 10) {
            throw new Exception("Weight needs to be in range (0;10]");
        }
        metaPathsWeights.set(index, weight);
    }
}
