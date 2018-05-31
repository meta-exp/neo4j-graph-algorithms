package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFullWeights;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * 5     5      5
 * (1)---(2)---(3)----.
 * 5/ 2    2     2     2 \     5
 * (0)---(7)---(8)---(9)---(10)-//->(0)
 * 3\    3     3     3   /
 * (4)---(5)---(6)----°
 * <p>
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */

public class ComputeAllMetaPathsSchemaFullWeightsTest {

    private static GraphDatabaseAPI api;
    private ComputeAllMetaPathsSchemaFullWeights algo;
    private final HashSet<String> metaPaths = new HashSet<>(Arrays.asList("-1|-10|-1|-10|-2", "-1|-10|-1|-10|-3", "-2|-10|-1", "-3|-10|-1", "-3|-10|-1|-10|-2|-10|-3|-10|-3|-10|-1|-10|-1"));

    @BeforeClass
    public static void setup() throws KernelException, Exception {
        final String cypher =
                "CREATE (a:A {name:\"a\"})\n" +
                        "CREATE (b:B {name:\"b\"})\n" +
                        "CREATE (c:A {name:\"c\"})\n" +
                        "CREATE (i:A {name:\"i\"})\n" +
                        "CREATE (k:B {name:\"k\"})\n" +
                        "CREATE (o:A {name:\"o\"})\n" +
                        "CREATE (s:C {name:\"s\"})\n" +
                        "CREATE (t:C {name:\"t\"})\n" +
                        "CREATE\n" +
                        "  (a)-[:TYPE1]->(t),\n" +
                        "  (a)-[:TYPE1]->(c),\n" +
                        "  (a)-[:TYPE1]->(b),\n" +
                        "  (a)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(t),\n" +
                        "  (c)-[:TYPE1]->(s),\n" +
                        "  (c)-[:TYPE1]->(b),\n" +
                        "  (i)-[:TYPE1]->(t),\n" +
                        "  (t)-[:TYPE1]->(s),\n" +
                        "  (t)-[:TYPE1]->(o),\n" +
                        "  (s)-[:TYPE1]->(k),\n" +
                        "  (k)-[:TYPE1]->(s)\n";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(GettingStartedProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Before
    public void setupMetaPaths() throws Exception {
        algo = new ComputeAllMetaPathsSchemaFullWeights(3, api);
        HashMap<Integer, String> idTypeMappingNodes = new HashMap<>();
        HashMap<Integer, String> idTypeMappingEdges = new HashMap<>();
        idTypeMappingNodes.put(-1, "A");
        idTypeMappingNodes.put(-2, "B");
        idTypeMappingNodes.put(-3, "C");
        idTypeMappingEdges.put(-10, "TYPE1");
        algo.setIDTypeMappingNodes(idTypeMappingNodes);
        algo.setIDTypeMappingEdges(idTypeMappingEdges);
    }

    //Is different to testComputeMetaPathWeights because getTwoMPWeights is an actual method and the tested methods in testComputeMetaPathWeights build on the results of it to compute the weights for longer m-p
    @Test
    public void testGetTwoLongMPWeights() throws InterruptedException {
        HashSet<Integer> nodeLabelIDs = new HashSet<>(Arrays.asList(-1, -2, -3));
        algo.setNodeLabelIDs(nodeLabelIDs);
        HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer, Integer>>> adjacentNodesDict = new HashMap<>();
        adjacentNodesDict.put(-1, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-2, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        adjacentNodesDict.put(-2, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        adjacentNodesDict.put(-3, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-2, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        algo.setAdjacentNodesDict(adjacentNodesDict);
        algo.getTwoMPWeights();
        HashMap<String, Double> actualTwoLongMPWeightDict = new HashMap<>();
        actualTwoLongMPWeightDict.put("-1|-10|-1", (double) 2 / 26);
        actualTwoLongMPWeightDict.put("-1|-10|-2", (double) 2 / 26);
        actualTwoLongMPWeightDict.put("-1|-10|-3", (double) 5 / 26);
        actualTwoLongMPWeightDict.put("-2|-10|-1", (double) 2 / 26);
        actualTwoLongMPWeightDict.put("-2|-10|-3", (double) 4 / 26);
        actualTwoLongMPWeightDict.put("-3|-10|-1", (double) 5 / 26);
        actualTwoLongMPWeightDict.put("-3|-10|-2", (double) 4 / 26);
        actualTwoLongMPWeightDict.put("-3|-10|-3", (double) 2 / 26);
        assertEquals(actualTwoLongMPWeightDict, algo.getTwoMPWeightDict());
    }

    @Test
    public void testComputeMetaPathWeights() throws InterruptedException {
        HashSet<Integer> nodeLabelIDs = new HashSet<>(Arrays.asList(-1, -2, -3));
        algo.setNodeLabelIDs(nodeLabelIDs);
        HashMap<Integer, HashSet<AbstractMap.SimpleEntry<Integer, Integer>>> adjacentNodesDict = new HashMap<>();
        adjacentNodesDict.put(-1, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-2, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        adjacentNodesDict.put(-2, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        adjacentNodesDict.put(-3, new HashSet<>(Arrays.asList(new AbstractMap.SimpleEntry<>(-1, -10), new AbstractMap.SimpleEntry<>(-2, -10), new AbstractMap.SimpleEntry<>(-3, -10))));
        algo.setAdjacentNodesDict(adjacentNodesDict);
        algo.computeMetaPathWeights(metaPaths);
        HashMap<String, Double> actualMetaPathWeightsDict = new HashMap<>();
        actualMetaPathWeightsDict.put("-1|-10|-1|-10|-2", ((double) 2 / 26) * ((double) 2 / 26));
        actualMetaPathWeightsDict.put("-1|-10|-1|-10|-3", ((double) 2 / 26) * ((double) 5 / 26));
        actualMetaPathWeightsDict.put("-2|-10|-1", (double) 2 / 26);
        actualMetaPathWeightsDict.put("-3|-10|-1", (double) 5 / 26);
        actualMetaPathWeightsDict.put("-3|-10|-1|-10|-2|-10|-3|-10|-3|-10|-1|-10|-1", ((double) 5 / 26) * ((double) 2 / 26) * ((double) 4 / 26) * ((double) 2 / 26) * ((double) 5 / 26) * ((double) 2 / 26));
        assertEquals(actualMetaPathWeightsDict, algo.getMetaPathWeightsDict());
    }

    @Ignore //TODO could be a problem if we consider the direction of edges
    @Test
    public void testPairHashSet() {
        HashSet<AbstractMap.SimpleEntry<Integer, Integer>> nodeEdge = new HashSet<>();
        nodeEdge.add(new AbstractMap.SimpleEntry<>(1, 2));
        nodeEdge.add(new AbstractMap.SimpleEntry<>(1, 2));
        nodeEdge.add(new AbstractMap.SimpleEntry<>(2, 2));
        nodeEdge.add(new AbstractMap.SimpleEntry<>(1, 3));
        System.out.println(nodeEdge);
    }
}
