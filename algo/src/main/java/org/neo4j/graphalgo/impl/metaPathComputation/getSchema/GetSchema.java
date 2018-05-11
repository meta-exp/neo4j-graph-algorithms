package org.neo4j.graphalgo.impl.metaPathComputation.getSchema;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths;
import org.neo4j.graphalgo.impl.metaPathComputation.MetaPathComputation;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GetSchema extends MetaPathComputation {

    private HeavyGraph graph;
    private HashMap<Integer, Integer> labelDictionary;//maybe change to array if it stays integer->integer
    private HashMap<Integer, Integer> reverseLabelDictionary;//also change to Array
    private int amountOfLabels;
    private int numberOfCores;

    public GetSchema(HeavyGraph graph) {
        this.graph = graph;
        this.amountOfLabels = graph.getAllLabels().size();
        this.labelDictionary = new HashMap<>();
        this.reverseLabelDictionary = new HashMap<>();
        this.numberOfCores = Runtime.getRuntime().availableProcessors();
    }

    public Result compute() {
        initializeLabelDict();
        ArrayList<AddNeighboursToSchemaThread> threads = startThreads();
        joinThreads(threads);
        ArrayList<HashSet<Pair>> schema = mergeSchemata(threads);

        return new Result(schema, labelDictionary, reverseLabelDictionary);
    }

    private ArrayList<HashSet<Pair>> mergeSchemata(ArrayList<AddNeighboursToSchemaThread> threads) {
        ArrayList<HashSet<Pair>> schema = new ArrayList<>(amountOfLabels);//max supported nodecount = ca. 2.000.000.000
        for (int i = 0; i < amountOfLabels; i++) {
            HashSet<Pair> adjacencyRow = new HashSet<>();//maybe give size of amountOfLabels*amountOfEdgeLabels
            schema.add(adjacencyRow);
        }

        for (AddNeighboursToSchemaThread thread : threads) {
            ArrayList<HashSet<Pair>> threadSchema = thread.retrieveSchema();
            for (int i = 0; i < amountOfLabels; i++) {
                schema.get(i).addAll(threadSchema.get(i));
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
        for (int label : graph.getAllLabels()) {
            labelDictionary.put(label, labelCounter);
            reverseLabelDictionary.put(labelCounter, label);
            labelCounter++;
        }
    }

    private boolean addNeighboursToSchema(int node, ArrayList<HashSet<Pair>> schema) {
        int[] neighbours = graph.getOutgoingNodes(node);
        Integer[] labels = graph.getLabels(node);
        for (int label : labels) {
            Integer labelId = getLabelId(label);

            for (int neighbour : neighbours) {
                int edgeLabel = graph.getEdgeLabel(node, neighbour);

                Integer[] neighbourLabels = graph.getLabels(neighbour);
                for (int neighbourLabel : neighbourLabels) {
                    Integer neighbourLabelId = getLabelId(neighbourLabel);

                    Pair pair = new Pair(neighbourLabelId, edgeLabel);
                    schema.get(labelId).add(pair);

                    Pair pair2 = new Pair(labelId, edgeLabel);
                    schema.get(neighbourLabelId).add(pair2);
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

        public ArrayList<HashSet<Pair>> retrieveSchema() {
            return schema;
        }
    }


    //TODO------------------------------------------------------------------------------------------------------------------
    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeAllMetaPaths.Result(new HashSet<>()));
    }//no clue what this is all about

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
        HashMap<Integer, Integer> labelDictionary;
        HashMap<Integer, Integer> reverseLabelDictionary;

        public Result(ArrayList<HashSet<Pair>> schema, HashMap<Integer, Integer> labelDictionary, HashMap<Integer, Integer> reverseLabelDictionary) {
            this.schema = schema;
            this.labelDictionary = labelDictionary;
            this.reverseLabelDictionary = reverseLabelDictionary;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<HashSet<Pair>> getSchemaAdjacencies() {
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
