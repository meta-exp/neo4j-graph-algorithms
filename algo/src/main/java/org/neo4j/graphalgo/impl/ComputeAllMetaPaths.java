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
    private ArrayList<ArrayList<String>> metapaths;
    private ArrayList<Integer> metapathsWeights;
    private Random random;
    private int metaPathLength;
    private final static int DEFAULT_WEIGHT = 5;
    private HashMap<String, Byte> labelDictionary;
    private int[][] initialInstances;
    private final static int MAX_LABEL_COUNT = 50;
    private int max_instance_count;
    private byte currentLabelId = 0;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               HandyStuff handyStuff,
                               Degrees degrees, int metaPathLength, int max_label_count, int max_instance_count) throws IOException{

        this.graph = graph;
        this.handyStuff = handyStuff;
        this.degrees = degrees;
        this.metapaths = new ArrayList<>();
        this.metapathsWeights = new ArrayList<>();
        this.random = new Random();
        this.metaPathLength = metaPathLength;
        this.max_instance_count = max_instance_count;
        this.initialInstances = new int[max_instance_count][max_label_count]; //this wastes probably too much space for big graphs
        this.labelDictionary = new HashMap<>();
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.estimatedCount = Math.pow(max_label_count, metaPathLength + 1);

    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds){
        for(long l : incomingIds){
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetapaths();
        long endTime = System.nanoTime();

        System.out.println("calculation took: " + String.valueOf(endTime-startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime-startTime));

        //for (String metapath : finalMetaPaths) {
        //    out.println(metapath);
        //}

        startTime = System.nanoTime();
        System.out.println("Writing to disk took: " + String.valueOf(startTime-endTime));



/*

        for (String s:finalMetaPaths) {
            System.out.println(s + "\n");
        }
*/
        return new Result(finalMetaPaths);
    }

    public HashSet<String> computeAllMetapaths()
    {
        initializeLabelDictAndInitialInstances();
        computeMetapathsFromAllNodeLabels();

        //return collectMetapathsToStringsAndRemoveDuplicates();
        return duplicateFreeMetaPaths;
    }

    private void initializeLabelDictAndInitialInstances()
    {
        currentLabelId = 0;
        graph.forEachNode(node -> initializeNode(node));
    }

    private boolean initializeNode(int node)
    {
        String nodeLabel = handyStuff.getLabel(node);
        Byte nodeLabelId = labelDictionary.get(nodeLabel);

        if(nodeLabelId == null)
        {
            labelDictionary.put(nodeLabel, currentLabelId);
            nodeLabelId = currentLabelId;
            currentLabelId++;
            ArrayList<String> metapath = new ArrayList<>();
            metapath.add(nodeLabel);
            metapaths.add(metapath);
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetapath = String.join(" | ", metapath);
            duplicateFreeMetaPaths.add(joinedMetapath);
            int newSize = duplicateFreeMetaPaths.size();
            if(newSize > oldSize)
            {
                out.println(joinedMetapath);
                printCount++;
                if(printCount % ((int)estimatedCount/20) == 0) debugOut.println("Metapaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
            }
        }

        int instanceIndex = 0;
        while(instanceIndex < max_instance_count && initialInstances[instanceIndex][nodeLabelId] != 0) //maybe using arrays was a bad idea...
        {
            instanceIndex++;
        }
        initialInstances[instanceIndex][nodeLabelId] = node + 1; //to avoid nodeId 0, remember to subtract 1 later
        return true;
    }

    private void computeMetapathsFromAllNodeLabels()
    {
        for(String nodeLabel : handyStuff.getAllLabels()) {
            computeMetapathFromNodeLabel(nodeLabel, metaPathLength);
        }
    }

    private HashSet<String> collectMetapathsToStringsAndRemoveDuplicates()
    {
        HashSet<String> finalMetaPaths = new HashSet<>();
        for(ArrayList<String> metapath :metapaths){
            finalMetaPaths.add(String.join(" | ", metapath ));
        }
        return finalMetaPaths;
    }

    private void computeMetapathFromNodeLabel(ArrayList<String> currentMetaPath, int[] currentInstances, int metaPathLength)
    {
        if(metaPathLength == 0)
        {
            return;
        }

        ArrayList<ArrayList<Integer>> nextInstances = new ArrayList<>();
        for (int i = 0; i < labelDictionary.size(); i++) {
            nextInstances.add(new ArrayList<>());
        }

        for (int instance : currentInstances) {
            for (int nodeId : handyStuff.getAdjecentNodes(instance)) {
                nextInstances.get(labelDictionary.get(handyStuff.getLabel(nodeId))).add(nodeId);//get the id of the label of the node. add the node to the corresponding instances array
            }
        }

        for(int i = 0; i < nextInstances.size(); i++) { //replace with for each
            ArrayList<Integer> nextInstancesForLabel = nextInstances.get(i);
            if (!nextInstancesForLabel.isEmpty())
            {
                ArrayList<String> newMetaPath = new ArrayList<>();
                for (String label : currentMetaPath) {
                    newMetaPath.add(label);
                }
                newMetaPath.add(handyStuff.getLabel(nextInstancesForLabel.get(0)));//get(0) since all have the same label. mybe rename currentMetaPath?
                metapaths.add(newMetaPath);
                int oldSize = duplicateFreeMetaPaths.size();
                String joinedMetapath = String.join(" | ", newMetaPath );
                duplicateFreeMetaPaths.add(joinedMetapath);
                int newSize = duplicateFreeMetaPaths.size();
                if(newSize > oldSize)
                {
                    out.println(joinedMetapath);
                    printCount++;
                    if(printCount % ((int)estimatedCount/20) == 0) debugOut.println("Metapaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
                }
                int[] recursiveInstances = new int[nextInstancesForLabel.size()];//convert ArrayList<String> to  int[] array
                for (int j = 0; j < nextInstancesForLabel.size(); j++) {
                    recursiveInstances[j] = nextInstancesForLabel.get(j);
                }

                computeMetapathFromNodeLabel(newMetaPath, recursiveInstances, metaPathLength-1);  //do somehow dp instead?
            }
        }

    }

    private void computeMetapathFromNodeLabel(String startNodeLabel, int metaPathLength){

        ArrayList<String> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(startNodeLabel);

        int instanceIndex = 0;
        while(initialInstances[instanceIndex][labelDictionary.get(startNodeLabel)] != 0) //maybe using arrays was a bad idea...
        {
            instanceIndex++;
        }

        int[] initialInstancesRow = new int[instanceIndex];

        instanceIndex = 0;
        while(initialInstances[instanceIndex][labelDictionary.get(startNodeLabel)] != 0) //maybe using arrays was a bad idea...
        {
            initialInstancesRow[instanceIndex] = initialInstances[instanceIndex][labelDictionary.get(startNodeLabel)] - 1;// copy the initial instances of one common label
            instanceIndex++;
        }

        computeMetapathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
    }

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
        public Result(HashSet<String> finalMetaPaths)
        {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashSet<String> getFinalMetaPaths()
        {
            return finalMetaPaths;
        }


    }

    public void showTop(int n){
        for (int i = 0; i < n; i++){
            System.out.println(i + ". " + String.join(" | ", metapaths.get(i) ) + "  " + metapathsWeights.get(i));
        }
    }

    public void weight (int index, int weight) throws Exception {
        if(weight <= 0 || weight > 10)
            throw new Exception("Weight needs to be in range (0;10]");
        metapathsWeights.set(index, weight);
    }

    public float similarity (){
        float sum = 0;
        for (int weight: metapathsWeights){
            sum += weight;
        }
        return sum/metapathsWeights.size()/10;
    }

}
