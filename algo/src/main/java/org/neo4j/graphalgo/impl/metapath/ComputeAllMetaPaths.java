package org.neo4j.graphalgo.impl.metapath;

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import io.netty.util.collection.IntObjectMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.metapath.labels.LabelMapping;

import java.util.concurrent.*;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class ComputeAllMetaPaths extends MetaPathComputation {

    private HeavyGraph graph;
    private LabelMapping labelMapping;
    private int metaPathLength;
    private IntObjectMap<IntSet> initialInstances;
    private int currentLabelId = 0;
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private long startTime;
    private HashMap<Pair, Integer> labelDictionary;
    private short startEdgeLabel;

    public ComputeAllMetaPaths(HeavyGraph graph, LabelMapping labelMapping, int metaPathLength) throws IOException {
        this.graph = graph;
        this.labelMapping = labelMapping;
        startEdgeLabel = labelMapping.getAllEdgeLabels()[0];
        this.metaPathLength = metaPathLength;
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Debug.txt"));
        this.labelDictionary = new HashMap<>();
    }

    public Result compute() {
        debugOut.println("started computation");
        debugOut.println("length: " + metaPathLength);
        startTime = System.nanoTime();
        ArrayList<String> finalMetaPaths = computeAllMetaPaths();
        for (String mp : finalMetaPaths) {
            out.println(mp);
        }
        long endTime = System.nanoTime();

        System.out.println("calculation took: " + String.valueOf(endTime - startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime - startTime));
        debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    public ArrayList<String> computeAllMetaPaths() {

        initializeLabelDictAndInitialInstances();
        List<Runnable> threads = computeMetaPathsFromAllNodeLabels();
        mergeThreads(threads);

        return duplicateFreeMetaPaths;
    }

    private void mergeThreads(List<Runnable> threads) {
        for (Runnable thread : threads) {
            duplicateFreeMetaPaths.addAll(((ComputeMetaPathFromNodeLabelTask) thread).metaPaths);
        }
    }

    private int combineLabels(short firstLabel, short secondLabel) {
        return (int)firstLabel << 16 | (secondLabel & 0xFFFF);
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;
        HashMap<Integer, Integer> labelCountDict = new HashMap<>();

        for (int nodeLabel : labelMapping.getAllNodeLabels()) {
            for (int edgeLabel : labelMapping.getAllEdgeLabels()) {
                assignIdToNodeLabel(edgeLabel, nodeLabel);
            }
        }

        graph.forEachNode(node -> initializeNode(node, labelCountDict));
        for (int label : labelMapping.getAllNodeLabels()) {
            createMetaPathWithLengthOne(label, labelCountDict.get(label));
        }
    }

    private boolean initializeNode(int node, HashMap<Integer, Integer> labelCountDict) {
        short nodeLabel = labelMapping.getLabel(node);
        labelCountDict.put(nodeLabel, 1 + (labelCountDict.get(nodeLabel) == null ? 0 : labelCountDict.get(nodeLabel)));

        initialInstances.get(combineLabels(startEdgeLabel, nodeLabel)).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int edgeLabel, int nodeLabel) {
        labelDictionary.put(new Pair(edgeLabel, nodeLabel), currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

    private boolean createMetaPathWithLengthOne(int nodeLabel, int instanceCountSum) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addMetaPathGlobal(metaPath, instanceCountSum);
        return true;
    }

    private String addMetaPathGlobal(ArrayList<Integer> newMetaPath, long instanceCountSum) {
        String joinedMetaPath;
        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        joinedMetaPath += "\t" + instanceCountSum;
        duplicateFreeMetaPaths.add(joinedMetaPath);

        return joinedMetaPath;
    }

    private List<List<String>> computeMetaPathsFromAllNodeLabels() throws InterruptedException {
        int processorCount = Runtime.getRuntime().availableProcessors();
        debugOut.println("ProcessorCount: " + processorCount);

        ExecutorService executor = Executors.newFixedThreadPool(processorCount);
        List<Future<List<String>>> futures = new ArrayList<>();
        for (short nodeLabel : labelMapping.getAllNodeLabels()) {
            Future<T> future = executor.submit(new ComputeMetaPathFromNodeLabelTask(nodeLabel, metaPathLength));
            futures.add(future);
        }

        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.SECONDS);

        return futures.stream().map(this::get).collect(Collectors.toList());
    }

    private <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private ArrayList<HashMap<Integer, Integer>> allocateNextInstances() {
        int nextInstancesSize = labelMapping.getAllNodeLabels().size() * labelMapping.getAllEdgeLabels().size();
        ArrayList<HashMap<Integer, Integer>> nextInstances = new ArrayList<>(nextInstancesSize);

        for (int i = 0; i < nextInstancesSize; i++) {
            nextInstances.add(new HashMap<>());
        }

        return nextInstances;
    }
    static class Current {
        
    }

    private ArrayList<HashMap<Integer, Integer>> calculateNextInstances(HashMap<Integer, Integer> currentInstances) {
        ArrayList<HashMap<Integer, Integer>> nextInstances = allocateNextInstances();
        for (int instance : currentInstances.keySet()) {
            for (int nodeId : graph.getAdjacentNodes(instance)) {
                int label = labelMapping.getLabel(nodeId); //get the id of the label of the node
                int edgeLabel = labelMapping.getEdgeLabel(instance, nodeId);
                int labelID = labelDictionary.get(new Pair(edgeLabel, label));

                boolean incrementMissing = nextInstances.get(labelID).get(nodeId) == null;
                int oldCount = currentInstances.get(instance);
                int count = oldCount + (incrementMissing ? 0 : nextInstances.get(labelID).get(nodeId));

                nextInstances.get(labelID).put(nodeId, count); // add the node to the corresponding instances array
            }
        }
        return nextInstances;
    }

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        newMetaPath.addAll(currentMetaPath);

        return newMetaPath;
    }

    private HashMap<Integer, Integer> initInstancesRow(int startNodeLabel) {
        int startNodeLabelId = labelDictionary.get(new Pair(startEdgeLabel, startNodeLabel));
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        HashMap<Integer, Integer> dictRow = new HashMap<>();
        for (int instance : row) {
            dictRow.put(instance, 1);
        }
        return dictRow;
    }

    public static class MetaPath {
        short[] path;
        byte length;

        public MetaPath(short label) {
            this.path = new short[5];
            this.path[0] = label;
            this.length = 1;
        }

        public MetaPath() { }

        @Override
        public int hashCode() {
            if (length == 0) return 0;
            int hash = path[0];
            for (int i = 1; i < length; i++) {
                hash = hash*31 + i;
            }
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof MetaPath)) return false;
            MetaPath other = (MetaPath) o;
            if (length == 0) return other.length == 0;
            for (int i = 0; i < length; i++) {
                if (path[i] != other.path[i]) return false;
            }
            return true;
        }
        public String toString(Tokens labels, Tokens types) {
            StringBuilder sb = new StringBuilder(length * 10);
            for (int i = 0; i < length; i++) {
                int id = path[i];
                sb.append(i % 2 == 0 ? labels.name(id) : types.name(id));
                if (i < length -1) sb.append("->");
            }
            return sb.toString();
        }

        public void add(short label) {
            if (length == path.length) {
                path = Arrays.copyOf(path,length+5);
            }
            path[length++] = label;
        }

        public MetaPath copy() {
            MetaPath copy = new MetaPath();
            copy.length = length;
            int newLength = path.length - length >= 2 ? path.length : path.length + 2;
            copy.path = Arrays.copyOf(path, newLength);
            return copy;
        }

        public MetaPath extend(short edgeLabel, short nodeLabel) {
            MetaPath result = copy();
            result.add(edgeLabel);
            result.add(nodeLabel);
            return result;
        }
    }

    private class ComputeMetaPathFromNodeLabelTask implements Callable<ObjectIntMap<MetaPath>> {
        short nodeLabel;

        short metaPathLength;
        ObjectIntMap<MetaPath> metaPaths;

        ComputeMetaPathFromNodeLabelTask(short nodeLabel, short metaPathLength) {
            this.nodeLabel = nodeLabel;
            this.metaPathLength = metaPathLength;
            this.metaPaths = new ObjectIntHashMap<>();
        }

        public ObjectIntMap<MetaPath> call() {
            computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
            return metaPaths;
        }

        public void computeMetaPathFromNodeLabel(short startNodeLabel, short metaPathLength) {
            MetaPath initialMetaPath = new MetaPath(startNodeLabel);
            HashMap<Integer, Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
            computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);

        }

        private void computeMetaPathFromNodeLabel(MetaPath currentMetaPath, HashMap<Integer, Integer> currentInstances, int metaPathLength) {
            if (metaPathLength <= 0) {
                return;
            }

            ArrayList<HashMap<Integer, Integer>> nextInstances = calculateNextInstances(currentInstances);

            for (int edgeLabel : labelMapping.getAllEdgeLabels()) {
                for (short nodeLabel : labelMapping.getAllNodeLabels()) {
                    int instancesArrayIndex = labelDictionary.get(new Pair(edgeLabel, nodeLabel));
                    HashMap<Integer, Integer> nextInstancesForLabel = nextInstances.get(instancesArrayIndex);
                    if (!nextInstancesForLabel.isEmpty()) {
                        nextInstances.set(instancesArrayIndex, null);

                        MetaPath newMetaPath = currentMetaPath.extend(edgeLabel, nodeLabel);

                        if (metaPaths.containsKey(newMetaPath)) {
                            metaPaths.put(newMetaPath, metaPaths.get(newMetaPath) + 1);
                        } else {
                            metaPaths.put(newMetaPath, 1);
                        }

                        long instanceCountSum = 0;
                        for (int count : nextInstancesForLabel.values()) {//refactor to stream?
                            instanceCountSum += count;
                        }

                        ComputeAllMetaPaths.this.addMetaPathGlobal(newMetaPath, instanceCountSum);
                        computeMetaPathFromNodeLabel(newMetaPath, nextInstancesForLabel, metaPathLength - 1);
                    }
                }
            }
        }
    }


    //TODO------------------------------------------------------------------------------------------------------------------


    @Override
    public ComputeAllMetaPaths me() {
        return this;
    }

    @Override
    public ComputeAllMetaPaths release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        ArrayList<String> finalMetaPaths;

        public Result(ArrayList<String> finalMetaPaths) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public ArrayList<String> getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }
}
