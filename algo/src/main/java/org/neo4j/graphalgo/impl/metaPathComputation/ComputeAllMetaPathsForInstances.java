package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
//TODO test correctness!
public class ComputeAllMetaPathsForInstances extends MetaPathComputation {

    private Degrees degrees;
    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private ArrayList<Integer> metaPathsWeights;
    private int metaPathLength;
    private ArrayList<HashSet<Integer>> initialInstances;
    private int currentLabelId = 0;
    private HashMap<String, HashSet<Integer>> duplicateFreeMetaPaths = new HashMap<>();
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<Integer, Integer> labelDictionary;


    public ComputeAllMetaPathsForInstances(HeavyGraph graph, ArrayGraphInterface arrayGraphInterface, Degrees degrees, int metaPathLength) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Instances.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Instances_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);
        this.labelDictionary = new HashMap<>();
        this.degrees = degrees;
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        HashMap<String, HashSet<Integer>> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        System.out.println("calculation took: " + String.valueOf(endTime - startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime - startTime));
        debugOut.println("finished computation");
        duplicateFreeMetaPaths.forEach((a,b) -> out.print(a + "=" + b.stream().map(Object::toString).collect(Collectors.joining(",")) + "-"));
        out.print("\n");
        return new Result(finalMetaPaths);
    }

    public HashMap<String, HashSet<Integer>> computeAllMetaPaths() {

        initializeLabelDictAndInitialInstances();
        computeMetaPathsFromAllRelevantNodeLabels();

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;

        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            assignIdToNodeLabel(nodeLabel);
        }
        List<Integer> maxDegreeNodes = getMaxDegreeNodes();
        for (int node : maxDegreeNodes) {
            out.print(node + ":");
            initializeNode(node);
        }
    }

    private boolean initializeNode(int node) {
        int nodeLabel = arrayGraphInterface.getLabel(node);
        //createMetaPathWithLengthOne(nodeLabel);
        Integer nodeLabelId = labelDictionary.get(nodeLabel);//probably not the best way to initialize labelDictionary
        initialInstances.get(nodeLabelId).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int nodeLabel) {
        labelDictionary.put(nodeLabel, currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

   /* private void createMetaPathWithLengthOne(int nodeLabel) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath);
    }*/


    private void computeMetaPathsFromAllRelevantNodeLabels() {//TODO: rework for Instances
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;
        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread--" + i, nodeLabel, metaPathLength);
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

        while (!param1.empty() && !param2.empty() && !param3.empty()) {
            currentMetaPath = param1.pop();
            currentInstances = param2.pop();
            metaPathLength = param3.pop();

            if (metaPathLength <= 0) {
                continue;
            }

            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Length of currentInstances: " + currentInstances.size());
            //debugOut.println(Thread.currentThread().getName() + ": MetaPathLength: " + metaPathLength);
            //debugOut.println(Thread.currentThread().getName() + ": _________________");

            ArrayList<HashSet<Integer>> nextInstances = allocateNextInstances();
            //long startTime = System.nanoTime();
            fillNextInstances(currentInstances, nextInstances);
            //long endTime = System.nanoTime();
            //debugOut.println(((ComputeMetaPathFromNodeLabelThread) Thread.currentThread()).getThreadName() + ": Time for next instanceCalculation: " + (endTime - startTime));
            currentInstances = null;//not sure if this helps or not
            for (int i = 0; i < nextInstances.size(); i++) {
                HashSet<Integer> nextInstancesForLabel = nextInstances.get(i);
                if (!nextInstancesForLabel.isEmpty()) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int label = arrayGraphInterface.getLabel(nextInstancesForLabel.iterator().next()); //first element since all have the same label.
                    newMetaPath.add(label);
                    addAndLogMetaPath(newMetaPath, nextInstancesForLabel);

                    //nextInstances = null; // how exactly does this work?
                    param1.push(newMetaPath);
                    param2.push(nextInstancesForLabel);
                    param3.push(metaPathLength - 1);
                    //nextInstances.set(i, null);
                    //nextInstancesForLabel = null;
                }
            }
        }
    }

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath, HashSet<Integer> nextInstancesForLabel) {
        synchronized (duplicateFreeMetaPaths) {
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetaPath = addMetaPath(newMetaPath, nextInstancesForLabel);
            int newSize = duplicateFreeMetaPaths.size();
            if (newSize > oldSize)
                printMetaPathAndLog(joinedMetaPath);
        }
    }


    private ArrayList<HashSet<Integer>> allocateNextInstances() {
        ArrayList<HashSet<Integer>> nextInstances = new ArrayList<>(arrayGraphInterface.getAllLabels().size());
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            nextInstances.add(new HashSet<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashSet<Integer> currentInstances, ArrayList<HashSet<Integer>> nextInstances) {
        for (int instance : currentInstances) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjecentNodes works
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int labelID = labelDictionary.get(label);
                nextInstances.get(labelID).add(nodeId); // add the node to the corresponding instances array
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

    private String addMetaPath(ArrayList<Integer> newMetaPath, HashSet<Integer> nextInstancesForLabel) {
        String joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
        duplicateFreeMetaPaths.putIfAbsent(joinedMetaPath, nextInstancesForLabel);
        duplicateFreeMetaPaths.get(joinedMetaPath).addAll(nextInstancesForLabel);

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        //out.print(joinedMetaPath);
        printCount++;
        if (printCount % ((int) estimatedCount / 50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100 * printCount / estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    public void computeMetaPathFromNodeLabel(int startNodeLabel, int metaPathLength) {
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(startNodeLabel);
        HashSet<Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
    }

    private HashSet<Integer> initInstancesRow(int startNodeLabel) {
        int startNodeLabelId = labelDictionary.get(startNodeLabel);
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        return row;
    }

    //TODO------------------------------------------------------------------------------------------------------------------
    public Stream<org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public ComputeAllMetaPathsForInstances me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsForInstances release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        HashMap<String, HashSet<Integer>> finalMetaPaths;

        public Result(HashMap<String, HashSet<Integer>> finalMetaPaths) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashMap<String, HashSet<Integer>> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }

    public void weight(int index, int weight) throws Exception {
        if (weight <= 0 || weight > 10) {
            throw new Exception("Weight needs to be in range (0;10]");
        }
        metaPathsWeights.set(index, weight);
    }

    private List<Integer> getMaxDegreeNodes() {
        ArrayList<Integer> list = new ArrayList();
        List<Integer> maxDegreeNodes;
        Iterator<Node> nodes;
        graph.forEachNode(list::add);
        list.sort(new DegreeComparator(graph)); //TODO Use Array instead of list?


        maxDegreeNodes = list.subList((int) (list.size() - Math.ceil((double) list.size() / 1000)), list.size());
        for (int nodeID : maxDegreeNodes) { //TODO always consecutive? (without gap)
            debugOut.println("nodeID: " + nodeID + "; degree: " + graph.degree(nodeID, Direction.BOTH) + "; label: " + graph.getLabel(nodeID));

        }
        return maxDegreeNodes;
    }
}


class DegreeComparator implements Comparator<Integer> {
    HeavyGraph graph;

    DegreeComparator(HeavyGraph graph) {
        this.graph = graph;
    }

    @Override
    public int compare(Integer a, Integer b) {
        return Integer.compare(graph.degree(a, Direction.BOTH), graph.degree(b, Direction.BOTH));
    }
}