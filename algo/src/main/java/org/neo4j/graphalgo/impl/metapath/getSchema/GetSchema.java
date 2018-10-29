package org.neo4j.graphalgo.impl.metapath.getSchema;

import com.carrotsearch.hppc.IntIntHashMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metapath.MetaPathComputation;
import org.neo4j.graphalgo.impl.metapath.Pair;
import org.neo4j.graphalgo.impl.metapath.labels.LabelMapping;

import java.util.*;
import java.io.*;

public class GetSchema extends MetaPathComputation {

    private final LabelMapping mapping;
    private HeavyGraph graph;
    private IntIntHashMap labelDictionary;//maybe change to array if it stays integer->integer
    private HashMap<Integer, Integer> reversedLabelDictionary;//also change to Array
    private int amountOfLabels;
    private int numberOfCores;
    private PrintStream debugOut;
    private long startTime;
    private long endTime;

    public GetSchema(HeavyGraph graph, LabelMapping mapping) throws FileNotFoundException {
        this.graph = graph;
        this.mapping = mapping;
        this.amountOfLabels = mapping.getAllNodeLabels().length;
        this.labelDictionary = new IntIntHashMap();
        this.reversedLabelDictionary = new HashMap<>();
        this.numberOfCores = Runtime.getRuntime().availableProcessors();

        this.debugOut = new PrintStream(new FileOutputStream("Get_Schema_Debug.txt"));
    }

    public Result compute() {
        debugOut.println("START GET_SCHEMA");

        startTime = System.nanoTime();
        ArrayList<HashSet<Pair>> schema = computeSchema();
        endTime = System.nanoTime();

        debugOut.println("FINISH GET_SCHEMA after " + (endTime - startTime) / 1000000 + " milliseconds");
        return new Result(schema, labelDictionary, reversedLabelDictionary);
    }

    private ArrayList<HashSet<Pair>> computeSchema() {
        initializeLabelDict();
        ArrayList<AddNeighboursToSchemaThread> threads = startThreads();
        joinThreads(threads);
        ArrayList<HashSet<Pair>> schema = mergeSchemata(threads);

        try {
            FileOutputStream fileOut = new FileOutputStream("metagraph.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(schema);
            out.close();
            fileOut.close();

            fileOut = new FileOutputStream("reversedLabelDictionary.ser");
            out = new ObjectOutputStream(fileOut);
            out.writeObject(reversedLabelDictionary);
            out.close();
            fileOut.close();

        } catch (IOException i) {
            i.printStackTrace();
        }
        return schema;
    }

    private ArrayList<HashSet<Pair>> mergeSchemata(ArrayList<AddNeighboursToSchemaThread> threads) {
        ArrayList<HashSet<Pair>> schema = new ArrayList<>(amountOfLabels);//max supported nodecount = ca. 2.000.000.000
        for (int i = 0; i < amountOfLabels; i++) {
            HashSet<Pair> adjacencyRow = new HashSet<>();//maybe give size of amountOfLabels*amountOfEdgeLabels
            schema.add(adjacencyRow);
        }

        for (AddNeighboursToSchemaThread thread : threads) {
            ArrayList<HashSet<Pair>> threadSchema = thread.getSchema();
            for (int i = 0; i < amountOfLabels; i++) {
                schema.get(i).addAll(threadSchema.get(i));
                threadSchema.set(i, null);
            }
        }

        return schema;
    }

    private void joinThreads(ArrayList<AddNeighboursToSchemaThread> threads) {
        for (AddNeighboursToSchemaThread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<AddNeighboursToSchemaThread> startThreads() {
        long nodeCount = graph.nodeCount();
        long numberOfNodesPerCore = nodeCount / numberOfCores;

        ArrayList<AddNeighboursToSchemaThread> threads = new ArrayList<>(numberOfCores);
        for (int i = 0; i < numberOfCores; i++) {
            AddNeighboursToSchemaThread thread = new AddNeighboursToSchemaThread((int) (i * numberOfNodesPerCore), numberOfNodesPerCore);
            threads.add(thread);
            thread.start();
        }
        AddNeighboursToSchemaThread missingThread = new AddNeighboursToSchemaThread((int) (numberOfCores * numberOfNodesPerCore), nodeCount - (int) (numberOfCores * numberOfNodesPerCore));
        threads.add(missingThread);
        missingThread.start();
        return threads;
    }

    private void initializeLabelDict() {
        int labelCounter = 0;
        for (int label : mapping.getAllNodeLabels()) {
            labelDictionary.put(label, labelCounter);
            reversedLabelDictionary.put(labelCounter, label);
            labelCounter++;
        }
    }

    private boolean addNeighboursToSchema(int node, ArrayList<HashSet<Pair>> schema) {
        int[] neighbours = graph.getOutgoingNodes(node);
        short[] labels = mapping.getLabels(node);
        for (int label : labels) {
            Integer labelId = getLabelId(label);

            for (int neighbour : neighbours) {
                int edgeLabel = mapping.getEdgeLabel(node, neighbour);

                short[] neighbourLabels = mapping.getLabels(neighbour);
                for (int neighbourLabel : neighbourLabels) {
                    Integer neighbourLabelId = getLabelId(neighbourLabel);

                    Pair outgoingEdge = new Pair(neighbourLabelId, edgeLabel);
                    schema.get(labelId).add(outgoingEdge);

                    Pair incomingEdge = new Pair(labelId, edgeLabel);
                    schema.get(neighbourLabelId).add(incomingEdge);
                }
            }
        }

        return true;
    }

    private Integer getLabelId(Integer label) {
        return labelDictionary.get(label);
    }

    class AddNeighboursToSchemaThread extends Thread {
        private int startNode;
        private long numberOfNodes;
        private ArrayList<HashSet<Pair>> schema;

        AddNeighboursToSchemaThread(int startNode, long numberOfNodes) {
            this.startNode = startNode;
            this.numberOfNodes = numberOfNodes;
        }

        public void run() {
            ArrayList<HashSet<Pair>> schema = new ArrayList<>(amountOfLabels);//max supported nodecount = ca. 2.000.000.000
            for (int i = 0; i < amountOfLabels; i++) {
                HashSet<Pair> adjacencyRow = new HashSet<>();//maybe give size of amountOfLabels*amountOfEdgeLabels
                schema.add(adjacencyRow);
            }

            for (int i = startNode; i < startNode + numberOfNodes; i++) {
                addNeighboursToSchema(i, schema);
            }
            this.schema = schema;
        }

        public ArrayList<HashSet<Pair>> getSchema() {
            return schema;
        }
    }


    //------------------------------------------------------------------------------------------------------------------

    @Override
    public GetSchema me() {
        return this;
    }

    @Override
    public GetSchema release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        ArrayList<HashSet<Pair>> schema;
        IntIntHashMap labelDictionary;
        HashMap<Integer, Integer> reverseLabelDictionary;

        public Result(ArrayList<HashSet<Pair>> schema, IntIntHashMap labelDictionary, HashMap<Integer, Integer> reverseLabelDictionary) {
            this.schema = schema;
            this.labelDictionary = labelDictionary;
            this.reverseLabelDictionary = reverseLabelDictionary;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<HashSet<Pair>> getSchema() {
            return schema;
        }

        public IntIntHashMap getLabelDictionary() {
            return labelDictionary;
        }

        public HashMap<Integer, Integer> getReverseLabelDictionary() {
            return reverseLabelDictionary;
        }
    }
}
