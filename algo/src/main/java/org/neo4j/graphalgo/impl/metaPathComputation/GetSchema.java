package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GetSchema extends MetaPathComputation  {

    private HeavyGraph graph;
    private HashMap<Integer, Integer> labelDictionary;//maybe change to array if it stays integer->integer
    private HashMap<Integer, Integer> reverseLabelDictionary;//also change to Array
    private int labelCounter;

    public GetSchema(HeavyGraph graph) {
        this.graph = graph;
        this.labelDictionary = new HashMap<>();
        reverseLabelDictionary = new HashMap<>();
        this.labelCounter = 0;
    }

    public Result compute() {
        ArrayList<ArrayList<Pair>> schema = new ArrayList<>(); //max supported nodecount = ca. 2.000.000.000
        graph.forEachNode(node -> addNeighboursToShema(node, schema));

        return new Result(schema, labelDictionary, reverseLabelDictionary);
    }

    private boolean addNeighboursToShema(int node, ArrayList<ArrayList<Pair>> schema)
    {
        int[] neighbours = graph.getOutgoingNodes(node);
        Integer[] labels = graph.getLabels(node);

        for (int neighbour : neighbours) {
            int edgeLabel = graph.getEdgeLabel(node, neighbour); //why is this method taking longs??
            Integer[] neighbourLabels = graph.getLabels(neighbour);

            for (Integer label : labels) {
                Integer labelId = getLabelId(schema, label);

                for(int neighbourLabel : neighbourLabels) {
                    Integer neighbourLabelId = getLabelId(schema, neighbourLabel);

                    Pair pair = new Pair();
                    pair.setCar(neighbourLabelId);
                    pair.setCdr(edgeLabel);

                    schema.get(labelId).add(pair);
                }
            }
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

            ArrayList<Pair> adjacencyRow = new ArrayList<>();
            schema.add(adjacencyRow);
        }
        return labelId;
    }

    private class Pair {
        int car = 0;
        int cdr = 0;

        int car()
        {
            return car;
        }

        int cdr()
        {
            return cdr;
        }

        void setCar(int value)
        {
            car = value;
        }

        void setCdr(int value)
        {
            cdr = value;
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
