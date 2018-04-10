package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.GettingStartedProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

/**         5     5      5
 *      (1)---(2)---(3)----.
 *    5/ 2    2     2     2 \     5
 *  (0)---(7)---(8)---(9)---(10)-//->(0)
 *    3\    3     3     3   /
 *      (4)---(5)---(6)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */

@RunWith(Parameterized.class)
public class GettingStartedTest {

    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (s:Node {name:'s', blow: 2})\n" +
                        "CREATE (a:Node {name:'a', blow: 2})\n" +
                        "CREATE (b:Node {name:'b', blow: 2})\n" +
                        "CREATE (c:Node {name:'c', blow: 2})\n" +
                        "CREATE (d:Node {name:'d', blow: 2})\n" +
                        "CREATE (e:Node {name:'e', blow: 2})\n" +
                        "CREATE (f:Node {name:'f', blow: 2})\n" +
                        "CREATE (g:Node {name:'g', blow: 2})\n" +
                        "CREATE (h:Node {name:'h', blow: 2})\n" +
                        "CREATE (i:Node {name:'i', blow: 2})\n" +
                        "CREATE (x:Node {name:'x', blow: 2})\n" +
                        "CREATE" +

                        " (x)-[:TYPE {cost:5, blue: 1}]->(s),\n" + // creates cycle

                        " (s)-[:TYPE {cost:10, blue: 1}]->(a),\n" + // line 1
                        " (a)-[:TYPE {cost:5, blue: 1}]->(b),\n" +
                        " (b)-[:TYPE {cost:5, blue: 1}]->(c),\n" +
                        " (c)-[:TYPE {cost:5, blue: 1}]->(x),\n" +

                        " (s)-[:TYPE {cost:3, blue: 1}]->(d),\n" + // line 2
                        " (d)-[:TYPE {cost:3, blue: 1}]->(e),\n" +
                        " (e)-[:TYPE {cost:3, blue: 1}]->(f),\n" +
                        " (f)-[:TYPE {cost:3, blue: 1}]->(x),\n" +

                        " (s)-[:TYPE {cost:2, blue: 1}]->(g),\n" + // line 3
                        " (g)-[:TYPE {cost:2, blue: 1}]->(h),\n" +
                        " (h)-[:TYPE {cost:2, blue: 1}]->(i),\n" +
                        " (i)-[:TYPE {cost:2, blue: 1}]->(x)";

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

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"},
                new Object[]{"Huge"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testGettingStarted() throws Exception {

        final Consumer consumer = mock(Consumer.class);

        final String cypher = "CALL algo.gettingStarted.stream({graph:'"+graphImpl+"'}) YIELD hasEdge";

        api.execute(cypher).accept(row -> {
            final boolean hasEdges = row.getBoolean("hasEdge");
            consumer.test(hasEdges);
            return true;
        });

        // 4 steps from start to end max
        verify(consumer, times(1)).test(eq( true));

    }

    private interface Consumer {
        void test(boolean hasEdges);
    }

}