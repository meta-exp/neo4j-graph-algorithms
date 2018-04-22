
package org.neo4j.graphalgo.impl.metaPathComputation;

        import org.bouncycastle.crypto.OutputLengthException;
        import org.neo4j.graphdb.*;

        import org.neo4j.kernel.internal.GraphDatabaseAPI;

        import java.io.FileOutputStream;
        import java.io.PrintStream;
        import java.util.*;
        import java.util.concurrent.Semaphore;
        import java.util.stream.Collectors;
        import java.util.stream.IntStream;
        import java.util.stream.Stream;

        import static java.lang.Math.max;
        import static java.lang.Math.toIntExact;

public class ComputeAllMetaPathsSchemaFull extends MetaPathComputation {

    private int metaPathLength;
    private PrintStream debugOut;
    public GraphDatabaseAPI api;
    private HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer,Integer>>> adjacentNodesDict = new HashMap<>();
    //private HashMap<Integer, Label> nodeIDLabelsDict = new HashMap<Integer, Label>();
    private List<Node> nodes = null;
    private List<Relationship> rels = null;
    private HashSet<String> duplicateFreeMetaPaths = new HashSet<>();
    private PrintStream out;
    int printCount = 0;
    double estimatedCount;
    private long startTime;
    private HashMap<Integer, String> idTypeMappingNodes = new HashMap<>();
    private HashMap<Integer, String> idTypeMappingEdges = new HashMap<>();
    final int MAX_NOF_THREADS = 12; //TODO why not full utilization?
    final Semaphore threadSemaphore = new Semaphore(MAX_NOF_THREADS);
    private HashSet<Integer> nodeLabelIDs = new HashSet<>();
    private HashMap<String, Double> twoMPWeightDict = new HashMap<>();
    private HashMap<String, Integer> countSingleTwoMPDict = new HashMap<>();
    private HashMap<String, Double> metaPathWeightsDict = new HashMap<>();
    private Integer countAllTwoMP;

    public ComputeAllMetaPathsSchemaFull(int metaPathLength, GraphDatabaseAPI api) throws Exception {
        this.metaPathLength = metaPathLength;
        this.api = api;
        this.debugOut = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Full_Debug.txt"));
        this.out = new PrintStream(new FileOutputStream("Precomputed_MetaPaths_Schema_Full.txt"));//ends up in root/tests //or in dockerhome
    }

    public Result compute() throws Exception{
        debugOut.println("START");
        startTime = System.nanoTime();
        getMetaGraph();
        estimatedCount = Math.pow(nodes.size(), metaPathLength + 1);
        initializeDictionaries();
        ArrayList<ComputeMetaPathFromNodeLabelThread> threads = new ArrayList<>();
        int i = 0;

        for (Node node : nodes) {
            ComputeMetaPathFromNodeLabelThread thread = new ComputeMetaPathFromNodeLabelThread(this, "thread-" + i, toIntExact(node.getId()), metaPathLength);
            thread.start();
            threads.add(thread);
            i++;
        }

        try {
            for (ComputeMetaPathFromNodeLabelThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        computeMetaPathWeights(duplicateFreeMetaPaths);
        int numComputedMP = duplicateFreeMetaPaths.size();
        int numComputedWeights = metaPathWeightsDict.size();
        if (numComputedMP != numComputedWeights) {
            throw new OutputLengthException("Number of computed meta-paths (" + numComputedMP + ") is different to number of meta-paths with computed weight (" + numComputedWeights + ")!");
        }

        return new Result(duplicateFreeMetaPaths, idTypeMappingNodes, idTypeMappingEdges, metaPathWeightsDict);
    }

    private void getMetaGraph() throws Exception {
        org.neo4j.graphdb.Result result;
        try (Transaction tx = api.beginTx()) {
            result = api.execute("CALL apoc.meta.graph()");
            tx.success();
        }
        Map<String, Object> row = result.next();
        nodes = (List<Node>) row.get("nodes");
        rels = (List<Relationship>) row.get("relationships");
        for (Node node : nodes){
            String nodeType = node.getLabels().iterator().next().name();
            this.idTypeMappingNodes.put(toIntExact(node.getId()), nodeType);
            debugOut.println(nodeType);
        }
    }

    private void initializeDictionaries(){
        for (Node node : nodes) {
            int nodeID = toIntExact(node.getId());

            HashSet<AbstractMap.SimpleEntry<Integer, Integer>> adjNodesSet = new HashSet<>();
            adjacentNodesDict.putIfAbsent(toIntExact(nodeID), adjNodesSet);
            for (Relationship rel : rels) {
                debugOut.println(rel);
                debugOut.println(rel.getAllProperties());
                try {
                    int adjNodeID = toIntExact(rel.getOtherNodeId(node.getId()));
                    int adjEdgeID = toIntExact(rel.getId());
                    this.idTypeMappingEdges.put(toIntExact(adjEdgeID), rel.getType().name());
                    adjacentNodesDict.get(nodeID).add(new AbstractMap.SimpleEntry<>(adjNodeID, adjEdgeID));
                } catch (Exception e) {/*prevent duplicates*/}
            }
        }
        out.println(adjacentNodesDict);
    }

    public void computeMetaPathFromNodeLabel(int nodeID, int metaPathLength) { //TODO will it be faster if not node but nodeID with dicts?
        ArrayList<Integer> initialMetaPath = new ArrayList<>();
        initialMetaPath.add(nodeID); //because node is already type (of nodes in the real graph)
        computeMetaPathFromNodeLabel(initialMetaPath, nodeID, metaPathLength - 1);
    }

    private void computeMetaPathFromNodeLabel(ArrayList<Integer> pCurrentMetaPath, int pCurrentInstance, int pMetaPathLength) {
        Stack<ArrayList<Integer>> st_allMetaPaths = new Stack();
        Stack<Integer> st_currentNode = new Stack();
        Stack<Integer> st_metaPathLength = new Stack();
        st_allMetaPaths.push(pCurrentMetaPath);
        st_currentNode.push(pCurrentInstance);
        st_metaPathLength.push(pMetaPathLength);

        ArrayList<Integer> currentMetaPath;
        int currentInstance;
        int metaPathLength;

        while(!st_allMetaPaths.empty() && !st_currentNode.empty() && !st_metaPathLength.empty())
        {
            currentMetaPath = st_allMetaPaths.pop();
            currentInstance = st_currentNode.pop();
            metaPathLength = st_metaPathLength.pop();

            if (metaPathLength <= 0) {
                continue;
            }

            HashSet<AbstractMap.SimpleEntry<Integer, Integer>> outgoingEdges = new HashSet<>();
            outgoingEdges.addAll(adjacentNodesDict.get(currentInstance));
            for (AbstractMap.SimpleEntry<Integer, Integer> edge : outgoingEdges) {
                ArrayList<Integer> newMetaPath = copyMetaPath(currentMetaPath);
                int nodeID = edge.getKey();
                int edgeID = edge.getValue();
                newMetaPath.add(edgeID);
                newMetaPath.add(nodeID);

                synchronized (duplicateFreeMetaPaths) {
                    // add new meta-path to threads
                    String joinedMetaPath;
                    joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
                    duplicateFreeMetaPaths.add(joinedMetaPath);
                    out.println(joinedMetaPath);
                }

                st_allMetaPaths.push(newMetaPath);
                st_currentNode.push(nodeID);
                st_metaPathLength.push(metaPathLength - 1);
                //debugOut.println("finished recursion of length: " + (metaPathLength - 1));
            }
        }
        // System.out.println("These are all our metapaths from node "+ pCurrentInstance);
        //System.out.println(duplicateFreeMetaPaths);
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

    private ArrayList<Integer> copyMetaPath(ArrayList<Integer> currentMetaPath) {
        ArrayList<Integer> newMetaPath = new ArrayList<>();
        for (int label : currentMetaPath) {
            newMetaPath.add(label);
        }
        //debugOut.println("copied currentMetaPath");

        return newMetaPath;
    }

    private String addMetaPath(ArrayList<Integer> newMetaPath) {
        String joinedMetaPath;

        joinedMetaPath = newMetaPath.stream().map(Object::toString).collect(Collectors.joining("|"));
        duplicateFreeMetaPaths.add(joinedMetaPath);

        return joinedMetaPath;
    }

    private void printMetaPathAndLog(String joinedMetaPath) {
        out.println(joinedMetaPath);
        printCount++;
/*        if (printCount % ((int)estimatedCount/50) == 0) {
            debugOut.println("MetaPaths found: " + printCount + " estimated Progress: " + (100*printCount/estimatedCount) + "% time passed: " + (System.nanoTime() - startTime));
        }*/
    }

    public void getTwoMPWeights() throws InterruptedException {
        countAllTwoMP = 0;
        ArrayList<ComputeTwoMPWeightsThread> threads = new ArrayList<>(MAX_NOF_THREADS);

        List<HashSet<Integer>> labelIDSets = divideIntoThreadSetsInt(nodeLabelIDs);

        int i = 0;
        for (HashSet<Integer> labelIDSet : labelIDSets) {
            threadSemaphore.acquire();
            ComputeTwoMPWeightsThread thread = new ComputeTwoMPWeightsThread(this, "thread-" + i, labelIDSet);
            thread.start();
            threads.add(thread);
            i++;
            threadSemaphore.release();
        }
        try {
            for (ComputeTwoMPWeightsThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final int COUNT_ALL_TWO_MP = countAllTwoMP; //need to be final to be used in lambda expression
        countSingleTwoMPDict.forEach((twoMP, count) -> twoMPWeightDict.put(twoMP, (double) count / (COUNT_ALL_TWO_MP))); //not COUNT_ALL_TWO_MP * 2, because we already have the sum of twoMP and not the number of edges
    }

    public void computeTwoMPWeights(HashSet<Integer> labelIDSet) {
        org.neo4j.graphdb.Result result = null;

        for (int nodeID1 : labelIDSet) {
            String nodeLabel1 = idTypeMappingNodes.get(nodeID1);
            HashSet<AbstractMap.SimpleEntry<Integer, Integer>> adjacentNodes = adjacentNodesDict.get(nodeID1);
            for (AbstractMap.SimpleEntry<Integer, Integer> edgeNodePair : adjacentNodes) {
                int nodeID2 = edgeNodePair.getKey();
                String nodeLabel2 = idTypeMappingNodes.get(nodeID2);
                int edgeID1 = edgeNodePair.getValue();
                String edgeLabel1 = idTypeMappingEdges.get(edgeID1);
                try (Transaction tx = api.beginTx()) {
                    result = api.execute("MATCH (:`" + nodeLabel1 + "`)-[:`" + edgeLabel1 + "`]-(:`" + nodeLabel2 + "`) RETURN count(*)");
                    tx.success();
                }
                Map<String, Object> row = result.next();
                int countSingleTwoMP = toIntExact((long) row.get("count(*)"));
                String twoMP = nodeID1 + "|" + edgeID1 + "|" + nodeID2;

                synchronized (countSingleTwoMPDict) {
                    countSingleTwoMPDict.put(twoMP, countSingleTwoMP);
                }
                synchronized (countAllTwoMP) {
                    countAllTwoMP += countSingleTwoMP;
                }
            }
        }
    }

    //TODO arrayList/Array instead of string?
    public void computeMetaPathWeights(HashSet<String> metaPaths) throws InterruptedException {
        long startTime = System.nanoTime();
        getTwoMPWeights();
        ArrayList<ComputeWeightsThread> threads = new ArrayList<>(MAX_NOF_THREADS);

        List<HashSet<String>> metaPathsThreadSets = divideIntoThreadSetsStr(metaPaths);

        int i = 0;
        for (HashSet<String> metaPathsSet : metaPathsThreadSets) {
            threadSemaphore.acquire();
            ComputeWeightsThread thread = new ComputeWeightsThread(this, "thread-" + i, metaPathsSet);
            thread.start();
            threads.add(thread);
            i++;
            threadSemaphore.release();
        }
        try {
            for (ComputeWeightsThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        debugOut.println("Time for computation of weights: " + (endTime - startTime));
    }

    public void computeWeights(HashSet<String> metaPaths) {
        for (String metaPath : metaPaths) {
            double metaPathWeight = 1;
            int thirdDelimiterIndex = 0;
            int thirdDelimiterIndexOld;
            int lengthOfLastMPID = 0;
            do { //TODO end of meta-path -> indexOf
                thirdDelimiterIndexOld = thirdDelimiterIndex;
                thirdDelimiterIndex = metaPath.indexOf("|", metaPath.indexOf("|", metaPath.indexOf("|", thirdDelimiterIndexOld - lengthOfLastMPID) + 1) + 1); //thirdDelimiterIndex - lengthOfLastMPID because last node in in next iteration first node, + 1 because we do not want the same delimiter again
                if (thirdDelimiterIndex == -1) { //if indexOf returns -1 -> end of meta path
                    String twoMP = metaPath.substring(max(thirdDelimiterIndexOld - lengthOfLastMPID, 0), metaPath.length());
                    metaPathWeight *= twoMPWeightDict.get(twoMP);
                    break;
                }
                String twoMP = metaPath.substring(max(thirdDelimiterIndexOld - lengthOfLastMPID, 0), thirdDelimiterIndex);
                lengthOfLastMPID = (twoMP.length() - 1) - twoMP.lastIndexOf("|"); //- 1 because everything else is 0-indexed
                metaPathWeight *= twoMPWeightDict.get(twoMP);
            } while (true);
            //TODO make synchronization more efficient? batches?
            synchronized (metaPathWeightsDict) {
                metaPathWeightsDict.put(metaPath, metaPathWeight);
            }
        }
    }

    private List<HashSet<String>> divideIntoThreadSetsStr(HashSet<String> set) {
        List<HashSet<String>> threadSets = new ArrayList<>(MAX_NOF_THREADS);
        for (int k = 0; k < MAX_NOF_THREADS; k++) {
            threadSets.add(new HashSet<String>());
        }

        int index = 0;
        for (String element : set) {
            threadSets.get(index++ % MAX_NOF_THREADS).add(element);
        }
        return threadSets;
    }

    private List<HashSet<Integer>> divideIntoThreadSetsInt(HashSet<Integer> set) {
        List<HashSet<Integer>> threadSets = new ArrayList<>(MAX_NOF_THREADS);
        for (int k = 0; k < MAX_NOF_THREADS; k++) {
            threadSets.add(new HashSet<Integer>());
        }

        int index = 0;
        for (Integer element : set) {
            threadSets.get(index++ % MAX_NOF_THREADS).add(element);
        }
        return threadSets;
    }

    public void setIDTypeMappingNodes(HashMap<Integer, String> idTypeMappingNodes) {
        this.idTypeMappingNodes = idTypeMappingNodes;
    }

    public void setIDTypeMappingEdges(HashMap<Integer, String> idTypeMappingEdges) {
        this.idTypeMappingEdges = idTypeMappingEdges;
    }

    public void setNodeLabelIDs(HashSet<Integer> nodeLabelIDs) {
        this.nodeLabelIDs = nodeLabelIDs;
    }

    public void setAdjacentNodesDict(HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer, Integer>>> adjacentNodesDict) {
        this.adjacentNodesDict = adjacentNodesDict;
    }

    public HashMap<String, Double> getTwoMPWeightDict() {
        return twoMPWeightDict;
    }

    public HashMap<String, Double> getMetaPathWeightsDict() {
        return metaPathWeightsDict;
    }

    //TODO -------------------------------------------------------------------

    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public ComputeAllMetaPathsSchemaFull me() {
        return this;
    }

    @Override
    public ComputeAllMetaPathsSchemaFull release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        HashSet<String> finalMetaPaths;
        HashMap<Integer, String> idTypeMappingNodes;
        HashMap<Integer, String> idTypeMappingEdges;
        HashMap<String, Double> metaPathWeightsDict;

        public Result(HashSet<String> finalMetaPaths, HashMap<Integer, String> idTypeMappingNodes, HashMap<Integer, String> idTypeMappingEdges, HashMap<String, Double> metaPathWeightsDict) {
            this.finalMetaPaths = finalMetaPaths;
            this.idTypeMappingNodes = idTypeMappingNodes;
            this.idTypeMappingEdges = idTypeMappingEdges;
            this.metaPathWeightsDict = metaPathWeightsDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashSet<String> getFinalMetaPaths() {
            return finalMetaPaths;
        }

        public HashMap<Integer, String> getIDTypeNodeDict() {
            return idTypeMappingNodes;
        }

        public HashMap<Integer, String> getIDTypeEdgeDict() {
            return idTypeMappingEdges;
        }

        public HashMap<String, Double> getMetaPathWeightsDict() {
            return metaPathWeightsDict;
        }
    }
}