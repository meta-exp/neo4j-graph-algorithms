package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.HandyStuff;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;
import scala.Int;

import java.lang.reflect.Array;
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
    private final static int MAX_INSTANCE_COUNT = 1000;
    private byte currentLabelId = 0;


    public ComputeAllMetaPaths(HeavyGraph graph,IdMapping idMapping,
                               HandyStuff handyStuff,
                               Degrees degrees, int metaPathLength){

        this.graph = graph;
        this.handyStuff = handyStuff;
        this.degrees = degrees;
        this.metapaths = new ArrayList<>();
        this.metapathsWeights = new ArrayList<>();
        this.random = new Random();
        this.metaPathLength = metaPathLength;
        this.initialInstances = new int[MAX_INSTANCE_COUNT][MAX_LABEL_COUNT]; //this wastes probably too much space for big graphs
        this.labelDictionary = new HashMap<>();
    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds){
        for(long l : incomingIds){
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {

        HashSet<String> finalMetaPaths = computeAllMetapaths();

        for (String s:finalMetaPaths) {
            System.out.println(s);
        }

        return new Result();
    }

    public HashSet<String> computeAllMetapaths()
    {
        initializeLabelDictAndInitialInstances();
        computeMetapathsFromAllNodeLabels();

        return collectMetapathsToStringsAndRemoveDuplicates();
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
        }

        int instanceIndex = 0;
        while(instanceIndex < MAX_INSTANCE_COUNT && initialInstances[instanceIndex][nodeLabelId] != 0) //maybe using arrays was a bad idea...
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
            finalMetaPaths.add(String.join(" | ", metapath ) + "\n");
        }
        return finalMetaPaths;
    }

    private void computeMetapathFromNodeLabel(ArrayList<String> currentMetaPath, int[] currentInstances, int metaPathLength)
    {
        if(metaPathLength == 0 || currentInstances[0] == 0)
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
                currentMetaPath.add(handyStuff.getLabel(nextInstancesForLabel.get(0)));//get(0) since all have the same label. mybe rename currentMetaPath?
                metapaths.add(currentMetaPath);
                int[] recursiveInstances = new int[nextInstancesForLabel.size()];//convert ArrayList<String> to  int[] array
                for (int j = 0; j < nextInstancesForLabel.size(); j++) {
                    recursiveInstances[j] = nextInstancesForLabel.get(j);
                }

                computeMetapathFromNodeLabel(currentMetaPath, recursiveInstances, metaPathLength-1);  //do somehow dp instead?
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

        computeMetapathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength);
    }

    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new Result());
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

        public Result() {

        }

        @Override
        public String toString() {
            return "Result{}";
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
