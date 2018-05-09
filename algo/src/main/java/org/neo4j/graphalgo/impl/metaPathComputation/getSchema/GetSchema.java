package org.neo4j.graphalgo.impl.metaPathComputation.getSchema;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeMetaPathFromNodeLabelThread;
import org.neo4j.graphalgo.impl.metaPathComputation.MetaPathComputation;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GetSchema extends MetaPathComputation {

    private HeavyGraph graph;
    private ArrayList<ArrayList<HashSet<Integer> > > inSchema;
    private HashMap<Integer, Integer> labelDictionary;//maybe change to array if it stays integer->integer
    private HashMap<Integer, Integer> reverseLabelDictionary;//also change to Array
    private int labelCounter;
    private int amountOfLabels;
    private int numberOfCores;

    public GetSchema(HeavyGraph graph) {
        this.graph = graph;
        this.amountOfLabels = graph.getAllLabels().size();
        this.labelDictionary = new HashMap<>();
        this.reverseLabelDictionary = new HashMap<>();
        this.labelCounter = 0;
        this.numberOfCores = Runtime.getRuntime().availableProcessors();

        this.inSchema = new ArrayList<>(amountOfLabels);
        for (int i = 0; i < amountOfLabels; i++) {
            ArrayList<HashSet<Integer>> row = new ArrayList<>(amountOfLabels);
            for(int j = 0; j < amountOfLabels; j++) {
                row.add(new HashSet<>());
            }
            inSchema.add(row);
        }
    }

    public Result compute() {
        ArrayList<ArrayList<Pair>> schema = new ArrayList<>(amountOfLabels); //max supported nodecount = ca. 2.000.000.000
        long nodeCount = graph.nodeCount();
        long numberOfNodesPerCore = nodeCount / numberOfCores;

        ArrayList<AddNeighboursToSchemaThread> threads = new ArrayList<>(numberOfCores);
        for(int i = 0; i < numberOfCores; i++) {
            AddNeighboursToSchemaThread thread = new AddNeighboursToSchemaThread(schema, (int)(i * numberOfNodesPerCore), numberOfNodesPerCore);
            threads.add(thread);
            thread.start();
        }
        AddNeighboursToSchemaThread missingThread = new AddNeighboursToSchemaThread(schema, (int)(numberOfCores * numberOfNodesPerCore), nodeCount - (int)(numberOfCores * numberOfNodesPerCore));
        threads.add(missingThread);
        missingThread.start();

        for (AddNeighboursToSchemaThread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return new Result(schema, labelDictionary, reverseLabelDictionary);
    }

    private boolean addNeighboursToSchema(int node, ArrayList<ArrayList<Pair>> schema)
    {
        int[] neighbours = graph.getOutgoingNodes(node);
        Integer label = graph.getLabel(node);
        Integer labelId = getLabelId(schema, label);

        for (int neighbour : neighbours) {
            int edgeLabel = graph.getEdgeLabel(node, neighbour);

            Integer neighbourLabel = graph.getLabel(neighbour);
            Integer neighbourLabelId = getLabelId(schema, neighbourLabel);

            if(inSchema.get(labelId).get(neighbourLabelId).contains(edgeLabel)) continue;
            Pair pair = new Pair();
            pair.setCar(neighbourLabelId);
            pair.setCdr(edgeLabel);

            schema.get(labelId).add(pair);
            inSchema.get(labelId).get(neighbourLabelId).add(edgeLabel);

            if(inSchema.get(neighbourLabelId).get(labelId).contains(edgeLabel)) continue;
            Pair pair2 = new Pair();
            pair2.setCar(labelId);
            pair2.setCdr(edgeLabel);

            schema.get(neighbourLabelId).add(pair2);
            inSchema.get(neighbourLabelId).get(labelId).add(edgeLabel);
        }

        return true;
    }

    private Integer getLabelId(ArrayList<ArrayList<Pair>> schema, Integer label) {
        Integer labelId = labelDictionary.get(label);
        if(labelId == null)
        {
            labelDictionary.put(label, labelCounter);
            reverseLabelDictionary.put(labelCounter, label);
            labelId = labelCounter;
            labelCounter++;

            ArrayList<Pair> adjacencyRow = new ArrayList<>();//maybe give size of amountOfLabels*amountOfEdgeLabels
            schema.add(adjacencyRow);
        }
        return labelId;
    }

    class AddNeighboursToSchemaThread extends Thread {
        private int startNode;
        private long numberOfNodes;
        private ArrayList<ArrayList<Pair>> schema;

        AddNeighboursToSchemaThread(ArrayList<ArrayList<Pair>> schema, int startNode, long numberOfNodes) {
            this.startNode = startNode;
            this.numberOfNodes = numberOfNodes;
            this.schema = schema;
        }

        public void run() {
            for(int i = startNode; i < startNode + numberOfNodes; i++)
            addNeighboursToSchema(i, schema);
        }
    }


    //TODO------------------------------------------------------------------------------------------------------------------
    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeAllMetaPaths.Result(new HashSet<>()));
    }//no clue what this is all about

    @Override
    public GetSchema me() { return this; }

    @Override
    public GetSchema release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        ArrayList<ArrayList<Pair>> schema;
        HashMap<Integer, Integer> labelDictionary;
        HashMap<Integer, Integer> reverseLabelDictionary;

        public Result(ArrayList<ArrayList<Pair>> schema, HashMap<Integer, Integer> labelDictionary, HashMap<Integer, Integer> reverseLabelDictionary) {
            this.schema = schema;
            this.labelDictionary = labelDictionary;
            this.reverseLabelDictionary = reverseLabelDictionary;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<ArrayList<Pair>> getSchemaAdjacencies() {
            return schema;
        }

        public HashMap<Integer, Integer> getLabelDictionary() {
            return labelDictionary;
        }

        public HashMap<Integer, Integer> getReverseLabelDictionary() {
            return reverseLabelDictionary;
        }
    }
}
