package org.neo4j.graphalgo.impl.computeAllMetaPaths;

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
    private long max_instance_count; //TODO in this iteration of the code, there is no fixed amount of max_instances.
    private byte currentLabelId = 0;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               ArrayGraphInterface arrayGraphInterface,
                               Degrees degrees, int metaPathLength, long max_label_count, long max_instance_count) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.degrees = degrees;
        this.metaPaths = new ArrayList<>();
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.max_instance_count = max_instance_count;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(max_label_count, metaPathLength + 1);

    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds) {
        for (long l : incomingIds) {
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetapaths();
        long endTime = System.nanoTime();

        List<String> finalMetaPathsAsList = new ArrayList<>(finalMetaPaths) ;

        Collections.sort(finalMetaPathsAsList, (a, b) ->
                (int) a.toString().charAt(0) < (int) b.toString().charAt(0) ||
                (int) a.toString().charAt(0) == (int) b.toString().charAt(0) &&
                        (int) a.toString().charAt(a.toString().length()-1) < (int) b.toString().charAt(b.toString().length()-1) ? -1 :
                 (int) a.toString().charAt(0) == (int) b.toString().charAt(0) &&
                        (int) a.toString().charAt(a.toString().length()-1) == (int) b.toString().charAt(b.toString().length()-1) ? 0 : 1);

        for (String metaPath : finalMetaPathsAsList) {
            out.println(metaPath);
        }

        System.out.println("calculation took: " + String.valueOf(endTime-startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime-startTime));
        debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    public HashSet<String> computeAllMetapaths() {

        initializeLabelDictAndInitialInstances();
        computeMetaPathsFromAllNodeLabels();

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        graph.forEachNode(node -> initializeNode(node));
    }

    private boolean initializeNode(int node) {

        int nodeLabel = arrayGraphInterface.getLabel(node);
        createMetaPathWithLengthOne(nodeLabel);

        initialInstances.get(nodeLabel).add(node);
        return true;
    }

    private void createMetaPathWithLengthOne(int nodeLabel) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath);
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

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, HashSet<Integer> pCurrentInstances, int pMetaPathLength) {
        Stack<ArrayList<Integer>> param1 = new Stack();
        Stack<HashSet<Integer>> param2 = new Stack();
        Stack<Integer> param3 = new Stack();
        param1.push(pCurrentMetaPath);
        param2.push(pCurrentInstances);
        param3.push(pMetaPathLength);
        ArrayList<Integer> currentMetaPath;
        HashSet<Integer> currentInstances;
        int metaPathLength;

        while(!param1.empty() && !param2.empty() && !param3.empty())
        {
            currentMetaPath = param1.pop();
            currentInstances = param2.pop();
            metaPathLength = param3.pop();


            if (metaPathLength == 0) {
                //debugOut.println("aborting recursion");
                continue;
            }

            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Length of currentInstances: " + currentInstances.size());
            //debugOut.println(Thread.currentThread().getName() + ": MetaPathLength: " + metaPathLength);
            //debugOut.println(Thread.currentThread().getName() + ": _________________");


            ArrayList<HashSet<Integer>> nextInstances = allocateNextInstances();
            long startTime = System.nanoTime();
            fillNextInstances(currentInstances, nextInstances);
            long endTime = System.nanoTime();
            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Time for next instanceCalculation: " + (endTime - startTime));
            currentInstances = null;
            for (int i = 0; i < nextInstances.size(); i++) {
                HashSet<Integer> nextInstancesForLabel = nextInstances.get(i);
                if (!nextInstancesForLabel.isEmpty()) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int label = arrayGraphInterface.getLabel(nextInstancesForLabel.iterator().next()); //first element since all have the same label.
                    newMetaPath.add(label);
                    addAndLogMetaPath(newMetaPath);
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

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
        synchronized (duplicateFreeMetaPaths) {
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetaPath = addMetaPath(newMetaPath);
            int newSize = duplicateFreeMetaPaths.size();
            if (newSize > oldSize)
                printMetaPathAndLog(joinedMetaPath);
        }
    }


    private ArrayList<HashSet<Integer>> allocateNextInstances() {
        ArrayList<HashSet<Integer>> nextInstances = new ArrayList<>();//size ist schon bekannt
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            nextInstances.add(new HashSet<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashSet<Integer> currentInstances, ArrayList<HashSet<Integer>> nextInstances) {
        for (int instance : currentInstances) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjecentNodes works
                int labelID = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                nextInstances.get(labelID).add(nodeId); // add the node to the corresponding instances array
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

    private String addMetaPath(ArrayList<Integer> newMetaPath) {
        String joinedMetaPath;

        //metaPaths.add(newMetaPath);
        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
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
        HashSet<Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
        //debugOut.println("finished recursion for: " + startNodeLabel);
    }

    private HashSet<Integer> initInstancesRow(int startNodeLabel) {
        return initialInstances.get(startNodeLabel);
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
