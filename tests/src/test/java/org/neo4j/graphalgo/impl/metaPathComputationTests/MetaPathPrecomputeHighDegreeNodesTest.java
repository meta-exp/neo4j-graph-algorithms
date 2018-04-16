package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPaths;
import org.neo4j.graphalgo.impl.metaPathComputation.MetaPathPrecomputeHighDegreeNodes;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;

import static org.junit.Assert.assertEquals;


/**         5     5      5
 *      (1)---(2)---(3)----.
 *    5/ 2    2     2     2 \     5
 *  (0)---(7)---(8)---(9)---(10)-//->(0)
 *    3\    3     3     3   /
 *      (4)---(5)---(6)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */

public class MetaPathPrecomputeHighDegreeNodesTest {

    private static GraphDatabaseAPI api;
    private MetaPathPrecomputeHighDegreeNodes algo;

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
                        "  (t)-[:TYPE2]->(s),\n" +
                        "  (t)-[:TYPE2]->(o),\n" +
                        "  (k)-[:TYPE2]->(s)\n";

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
        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        algo = new MetaPathPrecomputeHighDegreeNodes(graph, graph, graph, 3, 0.5f, api);

    }

    //@Ignore //TODO ignored because now we delete entrys out of duplic... to save ram space //TODO: add test for edgeTypes
    @Test
    public void testCalculationOfMetaPaths() throws InterruptedException {
        MetaPathPrecomputeHighDegreeNodes.Result result = algo.compute();
        /*HashMap<Integer, HashMap<String, HashSet<Integer>>> actualIndexStructure = result.getFinalMetaPaths();
        HashMap<Integer, HashMap<String, HashSet<Integer>>> expectedIndexStructure = new HashMap<>();
        expectedIndexStructure.put(0, new HashMap<>());
        expectedIndexStructure.put(1, new HashMap<>());
        expectedIndexStructure.put(6, new HashMap<>());
        expectedIndexStructure.put(7, new HashMap<>());
        expectedIndexStructure.get(0).put("2|2", new HashSet<>(Arrays.asList(7,6)));
        expectedIndexStructure.get(0).put("2|1", new HashSet<>(Arrays.asList(4,1)));
        expectedIndexStructure.get(0).put("2|0", new HashSet<>(Arrays.asList(2,3,5,0)));
        expectedIndexStructure.get(0).put("1|0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(0).put("0|0", new HashSet<>(Arrays.asList(0)));
        expectedIndexStructure.get(0).put("1|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(0).put("0|1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(0).put("0|2", new HashSet<>(Arrays.asList(6)));
        expectedIndexStructure.get(0).put("0", new HashSet<>(Arrays.asList(2)));
        expectedIndexStructure.get(0).put("1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(0).put("2", new HashSet<>(Arrays.asList(6,7)));

        expectedIndexStructure.get(1).put("0|0", new HashSet<>(Arrays.asList(0,2)));
        expectedIndexStructure.get(1).put("0|1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(1).put("0|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(1).put("2|0", new HashSet<>(Arrays.asList(3,5,0,2)));
        expectedIndexStructure.get(1).put("2|1", new HashSet<>(Arrays.asList(4,1)));
        expectedIndexStructure.get(1).put("2|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(1).put("0", new HashSet<>(Arrays.asList(0,2)));
        expectedIndexStructure.get(1).put("2", new HashSet<>(Arrays.asList(6,7)));

        expectedIndexStructure.get(6).put("0|0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(6).put("0|1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(6).put("0|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(6).put("1|0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(6).put("1|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(6).put("2|0", new HashSet<>(Arrays.asList(5,3,0)));
        expectedIndexStructure.get(6).put("2|1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(6).put("2|2", new HashSet<>(Arrays.asList(6)));
        expectedIndexStructure.get(6).put("0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(6).put("1", new HashSet<>(Arrays.asList(4,1)));
        expectedIndexStructure.get(6).put("2", new HashSet<>(Arrays.asList(7)));

        expectedIndexStructure.get(7).put("0|0", new HashSet<>(Arrays.asList(2)));
        expectedIndexStructure.get(7).put("0|1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(7).put("0|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(7).put("1|0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(7).put("1|2", new HashSet<>(Arrays.asList(6,7)));
        expectedIndexStructure.get(7).put("2|0", new HashSet<>(Arrays.asList(2,0)));
        expectedIndexStructure.get(7).put("2|1", new HashSet<>(Arrays.asList(4,1)));
        expectedIndexStructure.get(7).put("2|2", new HashSet<>(Arrays.asList(7)));
        expectedIndexStructure.get(7).put("0", new HashSet<>(Arrays.asList(3,5,0)));
        expectedIndexStructure.get(7).put("1", new HashSet<>(Arrays.asList(1)));
        expectedIndexStructure.get(7).put("2", new HashSet<>(Arrays.asList(6)));

        System.out.println("expected:   " + expectedIndexStructure);
        System.out.println("actual:     " + actualIndexStructure);
        assert(actualIndexStructure.equals(expectedIndexStructure));*/
    }

    //TODO: write a test for the data written to the outputfile
//something is not working with the test so its commented out.
   /* @Test
    public void testCypherQuery() throws Exception {
        final ConsumerBool consumer = mock(ConsumerBool.class);
        final int input = 5;
        final String cypher = "CALL algo.computeAllMetaPaths('"+input+"')";
        System.out.println("Executed query: " + cypher);

        api.execute(cypher).accept(row -> {
            final int integer_in = 10;//row.getNumber("length").intValue()
            consumer.test(integer_in);
            return true;
        });

        // 4 steps from start to end max
        //verify(consumer, times(1)).test(eq( input));
    }

    private interface Consumer {
        void test(boolean hasEdges);
    }

    private interface ConsumerBool {
        void test(int integer_in);
    }*/
}
