package org.neo4j.graphalgo.impl.metaPathComputation;

        import org.neo4j.graphalgo.api.ArrayGraphInterface;
        import org.neo4j.graphalgo.api.IdMapping;
        import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
        import java.io.FileOutputStream;
        import java.io.PrintStream;
        import java.io.*;
        import java.util.*;
        import java.util.regex.Pattern;
        import java.util.stream.Collectors;
        import java.util.stream.IntStream;
        import java.util.stream.Stream;

public class ComputeAllMetaPathsForInstances extends MetaPathComputation {

    private HeavyGraph graph;
    private ArrayGraphInterface arrayGraphInterface;
    private ArrayList<Integer> metaPathsWeights;
    private int metaPathLength;
    private ArrayList<HashSet<Integer>> initialInstances;
    private int currentLabelId = 0;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    private PrintStream debugOut;
    private int printCount = 0;
    private double estimatedCount;
    private long startTime;
    private HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> labelDictionary;
    List<Integer> startNodes;
    List<Integer> endNodes;
    HashMap<Integer, HashSet<AbstractMap.SimpleEntry<ArrayList<Integer>, ArrayList<Integer>>>> highDegreeIndex;


    public ComputeAllMetaPathsForInstances(HeavyGraph graph, ArrayGraphInterface arrayGraphInterface, int metaPathLength, List<Integer> startNodes, List<Integer> endNodes) throws IOException {
        this.graph = graph;
        this.arrayGraphInterface = arrayGraphInterface;
        this.metaPathsWeights = new ArrayList<>();
        this.metaPathLength = metaPathLength;
        this.initialInstances = new ArrayList<>();
        for (int i = 0; i < arrayGraphInterface.getAllLabels().size() * arrayGraphInterface.getAllEdgeLabels().size(); i++) {
            this.initialInstances.add(new HashSet<>());
        }
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Instances.txt"));//ends up in root/tests //or in dockerhome
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Instances_Debug.txt"));
        this.estimatedCount = Math.pow(arrayGraphInterface.getAllLabels().size(), metaPathLength + 1);
        this.labelDictionary = new HashMap<>();
        this.highDegreeIndex = new HashMap<>();
        this.startNodes = startNodes;
        this.endNodes = endNodes;

        readPrecomputedData();
    }

    public Result compute() {
        debugOut.println("started computation");
        startTime = System.nanoTime();
        HashSet<String> finalMetaPaths = computeAllMetaPaths();
        long endTime = System.nanoTime();

        System.out.println("calculation took: " + String.valueOf(endTime-startTime));
        debugOut.println("actual amount of metaPaths: " + printCount);
        debugOut.println("total time past: " + (endTime-startTime));
        debugOut.println("finished computation");
        return new Result(finalMetaPaths);
    }

    public HashSet<String> computeAllMetaPaths() {
        initializeLabelDictAndInitialInstances();
        computeMetaPathsFromAllRelevantNodeLabels();

        return duplicateFreeMetaPaths;
    }

    private void readPrecomputedData() {
        try(BufferedReader br = new BufferedReader(new FileReader("Precomputed_MetaPaths_HighDegree.txt"))) {
            String line = br.readLine();

            while (line != null) {
                String[] parts = line.split(Pattern.quote(":"));
                String[] partsForInstance = parts[1].split(Pattern.quote("-"));
                HashSet<AbstractMap.SimpleEntry<ArrayList<Integer>, ArrayList<Integer>>> precomputedForInstance = new HashSet<>();
                for (String part : partsForInstance) {
                    String[] pair = part.split(Pattern.quote("="));

                    String[] pathElements = pair[0].split(Pattern.quote("|"));
                    ArrayList<Integer> pair0 = new ArrayList<>();
                    for (String pathElement : pathElements) {
                        pair0.add(Integer.valueOf(pathElement));
                    }
                    String[] endElements = pair[1].split(Pattern.quote(","));
                    ArrayList<Integer> pair1 = new ArrayList<>();
                    for (String endElement : endElements) {
                        pair1.add(Integer.valueOf(endElement));
                    }

                    AbstractMap.SimpleEntry<ArrayList<Integer>, ArrayList<Integer>> resultPair = new AbstractMap.SimpleEntry<>(pair0, pair1);
                    precomputedForInstance.add(resultPair);
                }
                ArrayList<Integer> endNodesForEmptyMetaPath = new ArrayList<>();
                endNodesForEmptyMetaPath.add(Integer.valueOf(parts[0]));
                precomputedForInstance.add(new AbstractMap.SimpleEntry<>(new ArrayList<>(), endNodesForEmptyMetaPath));

                highDegreeIndex.put(Integer.valueOf(parts[0]), precomputedForInstance);
                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initializeLabelDictAndInitialInstances() {
        currentLabelId = 0;

        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
                assignIdToNodeLabel(edgeLabel, nodeLabel);
            }
        }

        for (int node : startNodes) {
            initializeNode(node);
        }
    }

    private boolean initializeNode(int node) {
        int nodeLabel = arrayGraphInterface.getLabel(node);
        int edgeLabel = arrayGraphInterface.getAllEdgeLabels().iterator().next();
        //createMetaPathWithLengthOne(nodeLabel);
        Integer nodeLabelId = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel));
        initialInstances.get(nodeLabelId).add(node);
        return true;
    }

    private int assignIdToNodeLabel(int edgeLabel, int nodeLabel) {
        labelDictionary.put(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel), currentLabelId);
        currentLabelId++;
        return currentLabelId - 1;
    }

    private void createMetaPathWithLengthOne(int nodeLabel) {
        ArrayList<Integer> metaPath = new ArrayList<>();
        metaPath.add(nodeLabel);
        addAndLogMetaPath(metaPath);
    }


    private void computeMetaPathsFromAllRelevantNodeLabels() {//TODO: rework for Instances
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;
        for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread--" + i, nodeLabel, metaPathLength);
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

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> currentMetaPath, HashSet<Integer> currentInstances, int metaPathLength) {
        if (metaPathLength <= 0) {
            return;
        }

        ArrayList<HashSet<Integer>> nextInstances = allocateNextInstances();
        fillNextInstances(currentInstances, nextInstances, currentMetaPath, metaPathLength);

        currentInstances = null;//not sure if this helps or not

        for (int edgeLabel : arrayGraphInterface.getAllEdgeLabels()) {
            for (int nodeLabel : arrayGraphInterface.getAllLabels()) {
                int key = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, nodeLabel));
                HashSet<Integer> nextInstancesForLabel = nextInstances.get(key);
                if (!nextInstancesForLabel.isEmpty()) {
                    nextInstances.set(key, null);

                    ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                    newMetaPath.add(edgeLabel);
                    newMetaPath.add(nodeLabel);

                    for (int node : nextInstancesForLabel) {
                        if(endNodes.contains(node)) {
                            addAndLogMetaPath(newMetaPath);
                        }
                    }

                    computeMetaPathFromNodeLabel(newMetaPath, nextInstancesForLabel, metaPathLength - 1);

                    nextInstancesForLabel = null;
                }
            }
        }
    }

    private void addAndLogMetaPath(ArrayList<Integer> newMetaPath) {
        synchronized (duplicateFreeMetaPaths) {
            int oldSize = duplicateFreeMetaPaths.size();
            String joinedMetaPath = addMetaPath(newMetaPath);
            int newSize = duplicateFreeMetaPaths.size();
            if (newSize > oldSize)
                printMetaPathAndLog(joinedMetaPath);
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

    private void fillNextInstances(HashSet<Integer> currentInstances, ArrayList<HashSet<Integer>> nextInstances, ArrayList<Integer> currentMetaPath, int metaPathLength) {//TODO: refactor and rename
        for (int instance : currentInstances) {
            for (int nodeId : arrayGraphInterface.getAdjacentNodes(instance)) { //TODO: check if getAdjacentNodes works
                int label = arrayGraphInterface.getLabel(nodeId); //get the id of the label of the node
                int edgeLabel = arrayGraphInterface.getEdgeLabel(instance, nodeId);
                if (!highDegreeIndex.containsKey(nodeId)) {
                    int labelID = labelDictionary.get(new AbstractMap.SimpleEntry<>(edgeLabel, label));
                    nextInstances.get(labelID).add(nodeId); // add the node to the corresponding instances array
                }
                else
                {

                    for (AbstractMap.SimpleEntry<ArrayList<Integer>, ArrayList<Integer>> metaPathWithEnds : highDegreeIndex.get(nodeId)){
                        ArrayList<Integer> endCopy = (ArrayList<Integer>) metaPathWithEnds.getValue().clone();
                        endCopy.retainAll(endNodes);
                        if(!endCopy.isEmpty() && metaPathLength > metaPathWithEnds.getKey().size()/2);
                        {
                            ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                            newMetaPath.add(edgeLabel);
                            newMetaPath.add(label);
                            newMetaPath.addAll(metaPathWithEnds.getKey());
                            addAndLogMetaPath(newMetaPath);
                        }
                    }
                }
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

    private String addMetaPath(ArrayList<Integer> newMetaPath) {
        String joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining(" | "));
        duplicateFreeMetaPaths.add(joinedMetaPath);

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
        printCount++;
        if (printCount % ((int)estimatedCount/50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }
    }

    public void computeMetaPathFromNodeLabel(int startNodeLabel, int metaPathLength) {
            ArrayList<Integer> initialMetaPath = new ArrayList<>();
            initialMetaPath.add(startNodeLabel);
            HashSet<Integer> initialInstancesRow = initInstancesRow(startNodeLabel);
            computeMetaPathFromNodeLabel(initialMetaPath, initialInstancesRow, metaPathLength - 1);
    }

    private HashSet<Integer> initInstancesRow(int startNodeLabel) {
        int startEdgeLabel = arrayGraphInterface.getAllEdgeLabels().iterator().next();
        int startNodeLabelId = labelDictionary.get(new AbstractMap.SimpleEntry<>(startEdgeLabel, startNodeLabel));
        HashSet<Integer> row = initialInstances.get(startNodeLabelId);
        return row;
    }

    //TODO------------------------------------------------------------------------------------------------------------------
    public Stream<org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public ComputeAllMetaPathsForInstances me() { return this; }

    @Override
    public ComputeAllMetaPathsForInstances release() {
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

    public void weight (int index, int weight) throws Exception {
        if (weight <= 0 || weight > 10) {
            throw new Exception("Weight needs to be in range (0;10]");
        }
        metaPathsWeights.set(index, weight);
    }
}
