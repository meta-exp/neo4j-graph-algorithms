package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.impl.metaPathComputation.MetaPath;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;

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

public class MetaPathTest {

    private static GraphDatabaseAPI api;
    private MetaPath algo;

    @BeforeClass
    public static void setup() throws KernelException, Exception {
        final String cypher =
                        "CREATE (a:SNP {name:\"a\"})\n" +
                        "CREATE (b:PHN {name:\"b\"})\n" +
                        "CREATE (c:SNP {name:\"c\"})\n" +
                        "CREATE (d:PHN {name:\"d\"})\n" +
                        "CREATE (e:GEN {name:\"e\"})\n" +
                        "CREATE (f:SNP {name:\"f\"})\n" +
                        "CREATE (g:PHN {name:\"g\"})\n" +
                        "CREATE (h:GEN {name:\"h\"})\n" +
                        "CREATE (i:SNP {name:\"i\"})\n" +
                        "CREATE (j:GEN {name:\"j\"})\n" +
                        "CREATE (k:SNP {name:\"k\"})\n" +
                        "CREATE (l:GEN {name:\"l\"})\n" +
                        "CREATE (m:PHN {name:\"m\"})\n" +
                        "CREATE (n:SNP {name:\"n\"})\n" +
                        "CREATE (o:GEN {name:\"o\"})\n" +
                        "CREATE (p:PHN {name:\"p\"})\n" +
                        "CREATE (q:GEN {name:\"q\"})\n" +
                        "CREATE (r:SNP {name:\"r\"})\n" +
                        "CREATE (s:GEN {name:\"s\"})\n" +
                        "CREATE (t:PHN {name:\"t\"})\n" +
                        "CREATE\n" +
                        "  (a)-[:TYPE1]->(t),\n" +
                        "  (a)-[:TYPE1]->(c),\n" +
                        "  (a)-[:TYPE1]->(b),\n" +
                        "  (a)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(t),\n" +
                        "  (c)-[:TYPE1]->(s),\n" +
                        "  (c)-[:TYPE1]->(h),\n" +
                        "  (l)-[:TYPE1]->(b),\n" +
                        "  (h)-[:TYPE1]->(l),\n" +
                        "  (c)-[:TYPE1]->(b),\n" +
                        "  (d)-[:TYPE1]->(a),\n" +
                        "  (d)-[:TYPE1]->(b),\n" +
                        "  (e)-[:TYPE1]->(b),\n" +
                        "  (e)-[:TYPE1]->(d),\n" +
                        "  (e)-[:TYPE1]->(f),\n" +
                        "  (f)-[:TYPE1]->(b),\n" +
                        "  (f)-[:TYPE1]->(c),\n" +
                        "  (g)-[:TYPE1]->(b),\n" +
                        "  (g)-[:TYPE1]->(e),\n" +
                        "  (h)-[:TYPE1]->(b),\n" +
                        "  (h)-[:TYPE1]->(e),\n" +
                        "  (i)-[:TYPE1]->(b),\n" +
                        "  (i)-[:TYPE1]->(e),\n" +
                        "  (i)-[:TYPE1]->(t),\n" +
                        "  (j)-[:TYPE1]->(s),\n" +
                        "  (j)-[:TYPE1]->(e),\n" +
                        "  (t)-[:TYPE1]->(o),\n" +
                        "  (k)-[:TYPE1]->(e),\n" +
                        "  (m)-[:TYPE2]->(n),\n" +
                        "  (m)-[:TYPE2]->(q),\n" +
                        "  (m)-[:TYPE2]->(p),\n" +
                        "  (m)-[:TYPE2]->(o),\n" +
                        "  (n)-[:TYPE2]->(k),\n" +
                        "  (k)-[:TYPE2]->(r),\n" +
                        "  (s)-[:TYPE1]->(m)\n";

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
        final Label snpLabel = Label.label("SNP");
        final Label genLabel = Label.label("GEN");
        final HashSet<Long> startNodeIds = new HashSet<>();
        final HashSet<Long> endNodeIds = new HashSet<>();

        try (Transaction tx = api.beginTx()) {
            startNodeIds.add(api.findNode(snpLabel, "name", "c").getId());
            startNodeIds.add(api.findNode(snpLabel, "name", "a").getId());
            endNodeIds.add(api.findNode(genLabel, "name", "o").getId());
            endNodeIds.add(api.findNode(genLabel, "name", "l").getId());
        }

        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);


        algo = new MetaPath(graph, graph, graph, startNodeIds, endNodeIds, 10, 8);

        // Something does not work: e.g. 0 - 0 - 0 - 0
        algo.compute();

    }

    @Test
    public void testSimilarityMeasure() throws Exception {
        assertEquals(0.5, algo.similarity(), 0);
    }

}
