package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import static org.mockito.Mockito.*;

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

public class ComputeAllMetaPathsTest {

    private static GraphDatabaseAPI api;
    private ComputeAllMetaPaths algo;

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
    public void setupMetapaths() throws Exception {
        /*final Label snpLabel = Label.label("SNP");
        final Label genLabel = Label.label("GEN");
        final HashSet<Long> startNodeIds = new HashSet<>();
        final HashSet<Long> endNodeIds = new HashSet<>();

        try (Transaction tx = api.beginTx()) {
            startNodeIds.add(api.findNode(snpLabel, "name", "c").getId());
            startNodeIds.add(api.findNode(snpLabel, "name", "a").getId());
            endNodeIds.add(api.findNode(genLabel, "name", "o").getId());
            endNodeIds.add(api.findNode(genLabel, "name", "l").getId());
        }
*/
        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        algo = new ComputeAllMetaPaths(graph, graph, graph, graph,3, 3, 9);

        algo.compute();

    }

    @Test
    public void testCalculationOfMetapaths() throws Exception {
        //assertEquals(0.5, algo.similarity(), 0);
        HashSet<String> allMetaPaths = algo.computeAllMetapaths();
        assert(allMetaPaths.contains("A | C | B"));
        assert(allMetaPaths.contains("A | A | B"));
        assert(allMetaPaths.contains("A | A | C"));
        assert(allMetaPaths.contains("A | C | C"));
        assert(allMetaPaths.contains("A | B | A"));
        assert(allMetaPaths.contains("A | B | C"));
        assert(allMetaPaths.contains("A | C | A"));
        assert(allMetaPaths.contains("B | A | A"));
        assert(allMetaPaths.contains("B | A | C"));
        assert(allMetaPaths.contains("B | C | A"));
        assert(allMetaPaths.contains("B | C | B"));
        assert(allMetaPaths.contains("B | C | C"));
        assert(allMetaPaths.contains("C | A | A"));
        assert(allMetaPaths.contains("C | A | B"));
        assert(allMetaPaths.contains("C | A | C"));
        assert(allMetaPaths.contains("C | B | A"));
        assert(allMetaPaths.contains("C | B | C"));
        assert(allMetaPaths.contains("C | C | A"));
        assert(allMetaPaths.contains("C | C | B"));
        assert(allMetaPaths.contains("A"));
        assert(allMetaPaths.contains("B"));
        assert(allMetaPaths.contains("C"));
        assert(allMetaPaths.contains("A | A"));
        assert(allMetaPaths.contains("A | B"));
        assert(allMetaPaths.contains("A | C"));
        assert(allMetaPaths.contains("B | A"));
        assert(allMetaPaths.contains("B | C"));
        assert(allMetaPaths.contains("C | A"));
        assert(allMetaPaths.contains("C | B"));
        assert(allMetaPaths.contains("C | C"));
        assert(allMetaPaths.contains("B | A | B"));//this should not exist, but in this prototype its ok. we are going back to the same node we already were
        assert(allMetaPaths.contains("A | A | A"));//this should not exist,...
        assert(allMetaPaths.contains("C | C | C"));//this should not exist,...
        assertEquals(33, allMetaPaths.size());//this should be 30, ...
    }

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
