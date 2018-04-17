package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
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
    private HashSet<String> duplicateFreeMetaPaths;
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> labelDictionary;

    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
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
        this.duplicateFreeMetaPaths = new HashSet<>();
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        List<String> finalMetaPathsAsList = new ArrayList<>(finalMetaPaths) ;

        //Collections.sort(finalMetaPathsAsList, (a, b) -> metaPathCompare(a.toString(), b.toString()));//TODO: write test for sort

        System.out.println("calculation took: " + String.valueOf(endTime-startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime-startTime));
        debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    private int metaPathCompare(String a, String b) {
        String[] partsInitA = a.split(Pattern.quote("\t"));
        String[] partsInitB = b.split(Pattern.quote("\t"));
        String[] partsA = partsInitA[0].split(Pattern.quote(" | "));
        String[] partsB = partsInitB[0].split(Pattern.quote(" | "));
        boolean firstLabelEqual = Integer.parseInt(partsA[0]) == Integer.parseInt(partsB[0]);
        boolean lastLabelEqual = Integer.parseInt(partsA[partsA.length - 1]) == Integer.parseInt(partsB[partsB.length - 1]);
        boolean firstLabelSmaller = Integer.parseInt(partsA[0]) < Integer.parseInt(partsB[0]);
        boolean lastLabelSmaller = Integer.parseInt(partsA[partsA.length - 1]) < Integer.parseInt(partsB[partsB.length - 1]);

        return  firstLabelSmaller || (firstLabelEqual && lastLabelSmaller) ? -1 :
                firstLabelEqual && lastLabelEqual ? 0 : 1;
    }

    public HashSet<String> computeAllMetaPaths() {

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
        graph.forEachNode(node -> createMetaPathWithLengthOne(arrayGraphInterface.getLabel(node), labelCountDict.get(arrayGraphInterface.getLabel(node))));

    }

    private boolean initializeNode(int node, HashMap<Integer, Integer> labelCountDict) {
        int nodeLabel = arrayGraphInterface.getLabel(node);
        int edgeLabel = arrayGraphInterface.getAllEdgeLabels().iterator().next();
        labelCountDict.put(nodeLabel, 1 + (labelCountDict.get(nodeLabel) == null ? 0 : labelCountDict.get(nodeLabel)));


        Integer nodeLabelId = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel));
        initialInstances.get(nodeLabelId).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int edgeLabel, int nodeLabel) {
        labelDictionary.put(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel), currentLabelId);
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
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;
        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, nodeLabel, metaPathLength);
            thread.start();
            threads.add(thread);
            i++;
        }
        for (ComputeMetaPathFromNodeLabelThread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, HashMap<Integer, Integer> pCurrentInstances, int pMetaPathLength) {
        Stack<ArrayList<Integer>> param1 = new Stack();
        Stack<HashMap<Integer, Integer>> param2 = new Stack();
        Stack<Integer> param3 = new Stack();
        param1.push(pCurrentMetaPath);
        param2.push(pCurrentInstances);
        param3.push(pMetaPathLength);

        ArrayList<Integer> currentMetaPath;
        HashMap<Integer, Integer> currentInstances;
        int metaPathLength;

        while(!param1.empty() && !param2.empty() && !param3.empty())
        {
            currentMetaPath = param1.pop();
            currentInstances = param2.pop();
            metaPathLength = param3.pop();

            if (metaPathLength <= 0) {
                continue;
            }

            ArrayList<HashMap<Integer, Integer>> nextInstances = allocateNextInstances();
            fillNextInstances(currentInstances, nextInstances);
            currentInstances = null;//not sure if this helps or not

            for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
                for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
                    int key = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel));
                    HashMap<Integer, Integer> nextInstancesForLabel = nextInstances.get(key);
                    if (!nextInstancesForLabel.isEmpty()) {
                        nextInstances.set(key, null);

                        ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                        newMetaPath.add(edgeLabel);
                        newMetaPath.add(nodeLabel);

                        long instanceCountSum = 0;
                        for (int count : nextInstancesForLabel.values()) {//refactor to stream?
                            instanceCountSum += count;
                        }

                        addAndLogMetaPath(newMetaPath, instanceCountSum);

                        //nextInstances = null; // how exactly does this work?
                        //computeMetaPathFromNodeLabel(newMetaPath, nextInstancesForLabel, metaPathLength-1);  //do somehow dp instead?
                        param1.push(newMetaPath);
                        param2.push(nextInstancesForLabel);
                        param3.push(metaPathLength - 1);
                        //debugOut.println("finished recursion of length: " + (metaPathLength - 1));
                        //nextInstances.set(i, null);
                        //nextInstancesForLabel = null;
                    }
                }
            }
        }
    }

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum) {
        synchronized (duplicateFreeMetaPaths) {
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetaPath = addMetaPath(newMetaPath, instanceCountSum);
            int newSize = duplicateFreeMetaPaths.size();
            if (newSize > oldSize)
                printMetaPathAndLog(joinedMetaPath);
        }
    }

    private ArrayList<HashMap<Integer, Integer>> allocateNextInstances() {
        int nextInstancesSize = arrayGraphInterface.getAllLabels().size() * arrayGraphInterface.getAllEdgeLabels().size();
        ArrayList<HashMap<Integer, Integer>> nextInstances = new ArrayList<>(nextInstancesSize);

        for (int i = 0; i < nextInstancesSize; i++) {
            nextInstances.add(new HashMap<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashMap<Integer,Integer> currentInstances, ArrayList<HashMap<Integer, Integer>> nextInstances) {
        for (int instance : currentInstances.keySet()) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) {
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int edgeLabel = arrayGraphInterface.getEdgeLabel(instance, nodeId);
                int labelID = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, label));

                boolean incrementMissing = nextInstances.get(labelID).get(nodeId) == null;
                int oldCount = currentInstances.get(instance);
                int count = oldCount + (incrementMissing ? 0 : nextInstances.get(labelID).get(nodeId));

                nextInstances.get(labelID).put(nodeId, count); // add the node to the corresponding instances array
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
        if (printCount % max(((int)estimatedCount/50), 1) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
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
        int startNodeLabelId = labelDictionary.get(new AbstractMap.SimpleEntry<>(startEdgeLabel, startNodeLabel));
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        HashMap<Integer, Integer> dictRow = new HashMap<>();
        for (int instance : row) {
            dictRow.put(instance, 1);
        }
        return dictRow;
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
}
