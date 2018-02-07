package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.ComputeAllMetaPaths;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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
                        "  (k)-[:TYPE1]->(s),\n";

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


        algo = new ComputeAllMetaPaths(graph, graph, graph, graph,3);

        algo.compute();

    }

    @Test
    public void testSimilarityMeasure() throws Exception {
        //assertEquals(0.5, algo.similarity(), 0);

    }

}
