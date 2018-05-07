package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Float.max;


public class GetSchema extends MetaPathComputation  {

    private HeavyGraph graph;
    private HashMap<Integer, Integer> labelDictionary;//maybe change to array if it stays integer->integer
    private int labelCounter;

    public GetSchema(HeavyGraph graph) {
        this.graph = graph;
        this.labelDictionary = new HashMap<>();
        this.labelCounter = 0;
    }

    public Result compute() {
        ArrayList<ArrayList<Pair>> schema = new ArrayList<>(); //max supported nodecount = ca. 2.000.000.000
        graph.forEachNode(node -> addNeighboursToShema(node, schema));

        return new Result(new ArrayList<>());
    }

    private boolean addNeighboursToShema(int node, ArrayList<ArrayList<Pair>> schema)
    {
        int[] neighbours = graph.getOutgoingNodes(node);
        Integer[] labels = graph.getLabels(node);

        for (Integer label : labels) {
            Integer labelId = labelDictionary.get(label);
            if(labelId == null)
            {
                labelDictionary.put(label, labelCounter);
                labelId = labelCounter;
                labelCounter++;

                ArrayList<Pair> adjacencyRow = new ArrayList<>();
                schema.add(adjacencyRow);
            }

            for (int neighbour : neighbours) {
                int edgeLabel = graph.getEdgeLabel(node, neighbour); //why is this method taking longs??

                Pair pair = new Pair();
                pair.setCar(neighbour);
                pair.setCdr(edgeLabel);
                
                schema.get(labelId).add(pair);
            }
        }

        return true;
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
        public Result(ArrayList<ArrayList<Pair>> schema) {
            this.schema = schema;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<ArrayList<Pair>> getSchemaAdjacencies() {
            return schema;
        }
    }
}
