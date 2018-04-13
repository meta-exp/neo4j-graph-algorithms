package org.neo4j.graphalgo.impl.metaPathComputation;


import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import scala.Int;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ComputeAllMetaPaths extends MetaPathComputation {

    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private Degrees degrees;
    private IdMap mapping;
    private ArrayList<ArrayList<Integer>> metaPaths;
    private int metaPathLength;
    private ArrayList<HashSet<Integer>> initialInstances;
    private int currentLabelId = 0;
    private HashMap<String, Integer> duplicateFreeMetaPaths;
    private PrintStream out;
    private PrintStream indexOut;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<Integer, Integer> labelDictionary;
    private HashMap<AbstractMap.SimpleEntry<Integer, Integer>, HashSet<Integer>> metaPathIndex;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               ArrayGraphInterface arrayGraphInterface,
                               Degrees degrees, int metaPathLength) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.degrees = degrees;
        this.metaPaths = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        this.indexOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Index.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);
        this.labelDictionary = new HashMap<>();
        this.metaPathIndex = new HashMap<>();
        this.duplicateFreeMetaPaths = new HashMap<>();
    }

    public void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds) {
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

        for (AbstractMap.SimpleEntry<Integer, Integer> pair: metaPathIndex.keySet()) {
            indexOut.println(pair + "\t" + metaPathIndex.get(pair));
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
        computeMetaPathsFromAllNodeLabels();

        return new HashSet<>(duplicateFreeMetaPaths.keySet());
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        HashMap<Integer, Integer> labelCountDict = new HashMap<>();
        graph.forEachNode(node -> initializeNode(node, labelCountDict));
        graph.forEachNode(node -> createMetaPathWithLengthOne(arrayGraphInterface.getLabel(node), labelCountDict.get(arrayGraphInterface.getLabel(node)), node, node));

    }

    private boolean initializeNode(int node, HashMap<Integer, Integer> labelCountDict) {

        int nodeLabel = arrayGraphInterface.getLabel(node);
        labelCountDict.put(nodeLabel, 1 + (labelCountDict.get(nodeLabel) == null ? 0 : labelCountDict.get(nodeLabel)));


        Integer nodeLabelId = labelDictionary.get(nodeLabel);//probably not the best way to initialize labelDictionary
        if (nodeLabelId == null) {
            nodeLabelId = assignIdToNodeLabel(nodeLabel);
        }
        initialInstances.get(nodeLabelId).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int nodeLabel) {
        labelDictionary.put(nodeLabel, currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

    private boolean createMetaPathWithLengthOne(int nodeLabel, int instanceCountSum, int startInstance, int endInstance) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath, instanceCountSum, startInstance, endInstance);
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

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> pCurrentInstances, int pMetaPathLength) {
        Stack<ArrayList<Integer>> param1 = new Stack();
        Stack<HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>>> param2 = new Stack();
        Stack<Integer> param3 = new Stack();
        param1.push(pCurrentMetaPath);
        param2.push(pCurrentInstances);
        param3.push(pMetaPathLength);

        ArrayList<Integer> currentMetaPath;
        HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> currentInstances;
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

            ArrayList<HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>>> nextInstances = allocateNextInstances();
            fillNextInstances(currentInstances, nextInstances);
            currentInstances = null;//not sure if this helps or not
            for (int i = 0; i < nextInstances.size(); i++) {
                HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> nextInstancesForLabel = nextInstances.get(i);
                if (!nextInstancesForLabel.isEmpty()) {
                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    int label = arrayGraphInterface.getLabel(nextInstancesForLabel.keySet().iterator().next()); //first element since all have the same label.
                    newMetaPath.add(label);
                    long instanceCountSum = 0;
                    for (AbstractMap.SimpleEntry<ArrayList<Integer>, Integer> pair : nextInstancesForLabel.values()) {//refactor to stream?
                        instanceCountSum += pair.getValue();
                    }

                    //addAndLogMetaPath(newMetaPath, instanceCountSum);
                    synchronized (duplicateFreeMetaPaths) {
                        int metaPathId = addMetaPath(newMetaPath, instanceCountSum);
                        for (int instance : nextInstancesForLabel.keySet()) {
                            for (int startInstance : nextInstancesForLabel.get(instance).getKey()) {
                                addMetaPathToIndex(startInstance, instance, metaPathId);
                            }
                        }
                    }

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

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum, int startInstance, int endInstance) {
        synchronized (duplicateFreeMetaPaths) {//TODO: check if metaPathIndex needs synchronized
            int metaPathId = addMetaPath(newMetaPath, instanceCountSum);
            addMetaPathToIndex(startInstance, endInstance, metaPathId);
        }
    }

    private void addMetaPathToIndex(int startInstance, int endInstance, int metaPathId) {
        AbstractMap.SimpleEntry<Integer, Integer> key = new AbstractMap.SimpleEntry<>(startInstance, endInstance);
        HashSet<Integer> metaPathIds  = metaPathIndex.getOrDefault(key, new HashSet<>());
        metaPathIds.add(metaPathId);
        metaPathIndex.put(key, metaPathIds);
    }


    private ArrayList<HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>>> allocateNextInstances() {
        ArrayList<HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>>> nextInstances = new ArrayList<>(arrayGraphInterface.getAllLabels().size());
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size(); i++) {
            nextInstances.add(new HashMap<>());
        }

        return nextInstances;
    }

    private void fillNextInstances(HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> currentInstances, ArrayList<HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>>> nextInstances) {
        for (int instance : currentInstances.keySet()) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjecentNodes works
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int labelID = labelDictionary.get(label);

                boolean incrementMissing = nextInstances.get(labelID).get(nodeId) == null;
                int oldCount = currentInstances.get(instance).getValue();
                ArrayList<Integer> oldSources = currentInstances.get(instance).getKey();

                ArrayList<Integer> sources = (ArrayList<Integer>)oldSources.clone();
                sources.addAll((incrementMissing ? new ArrayList<>() : nextInstances.get(labelID).get(nodeId).getKey()));
                int count = oldCount + (incrementMissing ? 0 : nextInstances.get(labelID).get(nodeId).getValue());

                nextInstances.get(labelID).put(nodeId, new AbstractMap.SimpleEntry<>(sources, count)); // add the node to the corresponding instances array
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

    private int addMetaPath(ArrayList<Integer> newMetaPath, long instanceCountSum) {
        String joinedMetaPath;
        //metaPaths.add(newMetaPath);
        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        joinedMetaPath += "\t" + instanceCountSum;
        Integer metaPathId = duplicateFreeMetaPaths.get(joinedMetaPath);
        if(metaPathId == null) {
            printCount++;
            duplicateFreeMetaPaths.put(joinedMetaPath, printCount);
            metaPathId = printCount;
            joinedMetaPath = metaPathId + ": " + joinedMetaPath;
            printMetaPathAndLog(joinedMetaPath);
        }
        return metaPathId;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
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
        HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> initialInstancesRow = initInstancesRow(startNodeLabel);
        computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
        //debugOut.println("finished recursion for: " + startNodeLabel);
    }

    private HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> initInstancesRow(int startNodeLabel) {
        int startNodeLabelId = labelDictionary.get(startNodeLabel);
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        HashMap<Integer,AbstractMap.SimpleEntry<ArrayList<Integer>, Integer>> dictRow = new HashMap<>();
        for (int instance : row) {
            ArrayList<Integer> sources = new ArrayList<>();
            sources.add(instance);
            dictRow.put(instance, new AbstractMap.SimpleEntry<>(sources ,1));
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
