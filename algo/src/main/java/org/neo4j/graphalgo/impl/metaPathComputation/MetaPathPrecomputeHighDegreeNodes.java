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
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


//TODO test correctness!

public class MetaPathPrecomputeHighDegreeNodes extends MetaPathComputation {

    private Degrees degrees;
    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private ArrayList<Integer> metaPathsWeights;
    private int metaPathLength;
    private int currentLabelId = 0;
    private HashMap<Integer, HashMap<String, HashSet<Integer>>> duplicateFreeMetaPaths;
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> labelDictionary;
    private float ratioHighDegreeNodes;
    private List<Integer> maxDegreeNodes;
    final int MAX_NOF_THREADS = 12; //TODO why not full utilization?
    final Semaphore threadSemaphore = new Semaphore(MAX_NOF_THREADS);


    public MetaPathPrecomputeHighDegreeNodes(HeavyGraph graph, ArrayGraphInterface arrayGraphInterface, Degrees degrees, int metaPathLength, float ratioHighDegreeNodes) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_HighDegree.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_HighDegree_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);
        this.labelDictionary = new HashMap<>();
        this.degrees = degrees;
        this.ratioHighDegreeNodes = ratioHighDegreeNodes;
        this.duplicateFreeMetaPaths = new HashMap<>();
    }

    public Result compute() throws InterruptedException{
        debugOut.println("started computation");
        startTime = System.nanoTime();
        maxDegreeNodes = getMaxDegreeNodes();
        HashMap<Integer, HashMap<String, HashSet<Integer>>> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        debugOut.println("calculation took: " + String.valueOf(endTime - startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime - startTime));
        debugOut.println("finished computation");

        System.out.println(endTime - startTime);
        return new Result(finalMetaPaths);
    }

    private void outputIndexStructure(int highDegreeNode, HashMap<String, HashSet<Integer>> metaPaths){
        synchronized(out) {
            out.print(highDegreeNode + ":");
            metaPaths.forEach((metaPath, endNodes) -> out.print(metaPath + "=" + endNodes.stream().map(Object::toString).collect(Collectors.joining(",")) + "-"));
            out.print("\n");
        }
    }

    public HashMap<Integer, HashMap<String, HashSet<Integer>>> computeAllMetaPaths() throws InterruptedException {

        initializeLabelDict();
        computeMetaPathsFromAllRelevantNodes();

        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDict() {
        currentLabelId = 0;

        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
                assignIdToNodeLabel(edgeLabel, nodeLabel);
            }
        }
    }

    private int assignIdToNodeLabel(int edgeLabel, int nodeLabel) {
        labelDictionary.put(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel), currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

    private void computeMetaPathsFromAllRelevantNodes() throws InterruptedException {//TODO: rework for Instances
        ArrayList<ComputeMetaPathFromNodeThread> threads = new ArrayList<>(maxDegreeNodes.size());
        int i = 0;
        for (int nodeID : maxDegreeNodes) {
            threadSemaphore.acquire();
            ComputeMetaPathFromNodeThread thread = new ComputeMetaPathFromNodeThread(this, "thread--" + i, nodeID, metaPathLength);
            thread.start();
            threads.add(thread);
            i++;

        }

        for (ComputeMetaPathFromNodeThread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> currentMetaPath, HashSet<Integer> currentInstances, int metaPathLength) {
        if (metaPathLength <= 0) {
            return;
        }

        ArrayList<HashSet<Integer>> nextInstances = allocateNextInstances();
        fillNextInstances(currentInstances, nextInstances);
        currentInstances = null;//not sure if this helps or not

        for(int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
            for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
                int key = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel));
                HashSet<Integer> nextInstancesForLabel = nextInstances.get(key);

                if(!nextInstancesForLabel.isEmpty()) {
                    nextInstances.set(key, null);

                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    newMetaPath.add(edgeLabel);
                    newMetaPath.add(nodeLabel);
                    addMetaPath(newMetaPath, nextInstancesForLabel);

                    computeMetaPathFromNodeLabel(newMetaPath, nextInstancesForLabel, metaPathLength - 1);
                    nextInstancesForLabel = null;
                }
            }
        }
    }

    private ArrayList<HashSet<Integer>> allocateNextInstances() {
        int nextInstancesSize = arrayGraphInterface.getAllLabels().size() * arrayGraphInterface.getAllEdgeLabels().size();
        ArrayList<HashSet<Integer>> nextInstances = new ArrayList<>(nextInstancesSize);

        for (int i = 0; i < nextInstancesSize; i++) {
            nextInstances.add(new HashSet<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashSet<Integer> currentInstances, ArrayList<HashSet<Integer>> nextInstances) {
        for (int instance : currentInstances) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjacentNodes works
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int edgeLabel = arrayGraphInterface.getEdgeLabel(instance, nodeId);
                int labelID = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, label));

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
        int nodeID = ((ComputeMetaPathFromNodeThread) Thread.currentThread()).getNodeID();
        duplicateFreeMetaPaths.get(nodeID).putIfAbsent(joinedMetaPath, nextInstancesForLabel);
        duplicateFreeMetaPaths.get(nodeID).get(joinedMetaPath).addAll(nextInstancesForLabel);

        return joinedMetaPath;
    }

    public void computeMetaPathFromNodeLabel(int nodeID, int metaPathLength) {
        duplicateFreeMetaPaths.put(nodeID, new HashMap<>());
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        //initialMetaPath.add(arrayGraphInterface.getLabel(nodeID)); //Not needed as the high degree node (start node) ID is given in the file and its label ID can be easily derived
        //TODO less hacky
        HashSet<Integer> initialInstaceRow = new HashSet<>();
        initialInstaceRow.add(nodeID);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstaceRow, metaPathLength - 1);
        outputIndexStructure(nodeID, duplicateFreeMetaPaths.get(nodeID));
        duplicateFreeMetaPaths.remove(nodeID);
        threadSemaphore.release();
    }
/*
    private HashSet<Integer> initInstancesRow(int startNodeLabel) {
        int startNodeLabelId = labelDictionary.get(startNodeLabel);
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        return row;
    }*/

    //TODO------------------------------------------------------------------------------------------------------------------
    public Stream<org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public MetaPathPrecomputeHighDegreeNodes me() {
        return this;
    }

    @Override
    public MetaPathPrecomputeHighDegreeNodes release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        HashMap<Integer, HashMap<String, HashSet<Integer>>> finalMetaPaths;

        public Result(HashMap<Integer, HashMap<String, HashSet<Integer>>> finalMetaPaths) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashMap<Integer, HashMap<String, HashSet<Integer>>> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }

    private List<Integer> getMaxDegreeNodes() {
        ArrayList<Integer> nodeList = new ArrayList();
        List<Integer> maxDegreeNodes;
        graph.forEachNode(nodeList::add);
        nodeList.sort(new DegreeComparator(graph)); //TODO Use Array instead of list?

        maxDegreeNodes = nodeList.subList((int) (nodeList.size() - Math.ceil((double) nodeList.size() * ratioHighDegreeNodes)), nodeList.size());
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