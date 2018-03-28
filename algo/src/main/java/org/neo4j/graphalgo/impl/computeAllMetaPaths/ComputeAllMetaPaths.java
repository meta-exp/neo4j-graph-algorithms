package org.neo4j.graphalgo.impl.computeAllMetaPaths;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Has;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ComputeAllMetaPaths extends Algorithm<ComputeAllMetaPaths> {

    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private Degrees degrees;
    private IdMap mapping;
    private ArrayList<ArrayList<Integer>> metaPaths;
    private ArrayList<Integer> metaPathsWeights;
    private int metaPathLength;
    private ArrayList<HashSet<Integer>> initialInstances;
    private byte currentLabelId = 0;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               ArrayGraphInterface arrayGraphInterface,
                               Degrees degrees, int metaPathLength) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.degrees = degrees;
        this.metaPaths = new ArrayList<>();
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);

    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds) {
        for (long l : incomingIds) {
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        List<String> finalMetaPathsAsList = new ArrayList<>(finalMetaPaths) ;

        //Collections.sort(finalMetaPathsAsList, (a, b) -> metaPathCompare(a.toString(), b.toString()));//TODO: write test for sort

        for (String metaPath : finalMetaPathsAsList) {
            out.println(metaPath);
        }

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
        //computeMetaPathsFromAllNodeLabels();

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        HashMap<Integer, Integer> labelCountDict = new HashMap<>();
        graph.forEachNode(node -> initializeNode(node, labelCountDict));
        for (int label : labelCountDict.keySet()) {
            createMetaPathWithLengthOne(label, labelCountDict.get(label));
        }
    }

    private boolean initializeNode(int node, HashMap<Integer, Integer> labelCountDict) {

        int nodeLabel = arrayGraphInterface.getLabel(node);
        labelCountDict.put(nodeLabel, 1 + (labelCountDict.get(nodeLabel) == null ? 0 : labelCountDict.get(nodeLabel)));

        initialInstances.get(nodeLabel).add(node);
        return true;
    }

    private void createMetaPathWithLengthOne(int nodeLabel, int instanceCountSum) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath, instanceCountSum);
    }


    private void computeMetaPathsFromAllNodeLabels() {
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;
        //debugOut.println("There are " + arrayGraphInterface.getAllLabels().size() + " labels.");
        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            //computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, nodeLabel, metaPathLength);
            thread.start();
            threads.add(thread);
            i++;
        }
        //debugOut.println("Created " + threads.size() + " threads.");
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
                //debugOut.println("aborting recursion");
                continue;
            }

            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Length of currentInstances: " + currentInstances.size());
            //debugOut.println(Thread.currentThread().getName() + ": MetaPathLength: " + metaPathLength);
            //debugOut.println(Thread.currentThread().getName() + ": _________________");


            ArrayList<HashMap<Integer, Integer>> nextInstances = allocateNextInstances();
            //long startTime = System.nanoTime();
            fillNextInstances(currentInstances, nextInstances);
            //long endTime = System.nanoTime();
            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Time for next instanceCalculation: " + (endTime - startTime));
            currentInstances = null;
            for (int i = 0; i < nextInstances.size(); i++) {
                HashMap<Integer, Integer> nextInstancesForLabel = nextInstances.get(i);
                if (!nextInstancesForLabel.isEmpty()) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int label = arrayGraphInterface.getLabel(nextInstancesForLabel.keySet().iterator().next()); //first element since all have the same label.
                    newMetaPath.add(label);
                    long instanceCountSum = 0;
                    for (int count : nextInstancesForLabel.values()) {
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
        ArrayList<HashMap<Integer, Integer>> nextInstances = new ArrayList<>(arrayGraphInterface.getAllLabels().size());
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            nextInstances.add(new HashMap<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashMap<Integer, Integer> currentInstances, ArrayList<HashMap<Integer, Integer>> nextInstances) {
        for (int instance : currentInstances.keySet()) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjecentNodes works
                int labelID = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                nextInstances.get(labelID).put(nodeId, currentInstances.get(instance) + (nextInstances.get(labelID).get(nodeId) == null ? 0 : nextInstances.get(labelID).get(nodeId))); // add the node to the corresponding instances array
            }
        }
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        for (int label : currentMetaPath) {
            newMetaPath.add(label);
        }
        //debugOut.println("copied currentMetaPath");

        return newMetaPath;
    }

    private String addMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum) {
        String joinedMetaPath;

        //metaPaths.add(newMetaPath);
        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        joinedMetaPath += "\t" + instanceCountSum;
        duplicateFreeMetaPaths.add(joinedMetaPath);
        //debugOut.println("tried adding new Metapath");

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        //out.println(joinedMetaPath);
        printCount++;
        if (printCount % ((int)estimatedCount/50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    /*private int[] convertArrayListToIntArray(ArrayList<Integer> nextInstancesForLabel) {
        int[] recursiveInstances = new int[nextInstancesForLabel.size()]; //convert ArrayList<String> to  int[] array //maybe this ist not necessary anymore. just change param
        for (int j = 0; j < nextInstancesForLabel.size(); j++) {
            recursiveInstances[j] = nextInstancesForLabel.get(j);
        }
        //debugOut.println("converted arrayList to int-array");

        return recursiveInstances;
    }*/

    public void computeMetaPathFromNodeLabel(int startNodeLabel, int metaPathLength) {
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(startNodeLabel);
        HashMap<Integer, Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
        //debugOut.println("finished recursion for: " + startNodeLabel);
    }

    private HashMap<Integer,Integer> initInstancesRow(int startNodeLabel) {
        HashSet<Integer> row = initialInstances.get(startNodeLabel);
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

    public void weight (int index, int weight) throws Exception {
        if (weight <= 0 || weight > 10) {
            throw new Exception("Weight needs to be in range (0;10]");
        }
        metaPathsWeights.set(index, weight);
    }
}
