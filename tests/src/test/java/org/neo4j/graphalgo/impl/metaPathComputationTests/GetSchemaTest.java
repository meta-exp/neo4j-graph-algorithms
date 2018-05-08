package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.GetSchema;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.impl.metaPathComputation.getSchema.Pair;

import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;

public class GetSchemaTest {
    private static GraphDatabaseAPI api;
    private GetSchema algo;
    private HeavyGraph graph;

    @BeforeClass
    public static void setup() throws Exception {
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
    public static void shutdownGraph() {
        api.shutdown();
    }

    @Before
    public void setupGraph() {
        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        algo = new GetSchema(graph);
    }

    @Ignore
    @Test
    public void testSchema() {
        GetSchema.Result result = algo.compute();
        ArrayList<ArrayList<Pair>> expectedSchema = new ArrayList<>();

        for(int i = 0; i < 3; i++) {
            expectedSchema.add(new ArrayList<>());
        }

        Pair pair = new Pair();
        pair.setCar(0);
        pair.setCdr(0);
        expectedSchema.get(0).add(pair);
        pair = new Pair();
        pair.setCar(2);
        pair.setCdr(0);
        expectedSchema.get(0).add(pair);
        pair = new Pair();
        pair.setCar(1);
        pair.setCdr(0);
        expectedSchema.get(0).add(pair);
        pair = new Pair();
        pair.setCar(1);
        pair.setCdr(1);
        expectedSchema.get(0).add(pair);

        pair = new Pair();
        pair.setCar(0);
        pair.setCdr(0);
        expectedSchema.get(2).add(pair);
        pair = new Pair();
        pair.setCar(1);
        pair.setCdr(0);
        expectedSchema.get(2).add(pair);
        pair = new Pair();
        pair.setCar(1);
        pair.setCdr(1);
        expectedSchema.get(2).add(pair);

        pair = new Pair();
        pair.setCar(0);
        pair.setCdr(0);
        expectedSchema.get(1).add(pair);
        pair = new Pair();
        pair.setCar(0);
        pair.setCdr(1);
        expectedSchema.get(1).add(pair);
        pair = new Pair();
        pair.setCar(2);
        pair.setCdr(0);
        expectedSchema.get(1).add(pair);
        pair = new Pair();
        pair.setCar(2);
        pair.setCdr(1);
        expectedSchema.get(1).add(pair);
        pair = new Pair();
        pair.setCar(1);
        pair.setCdr(1);
        expectedSchema.get(1).add(pair);

        ArrayList<ArrayList<Pair>> schema = result.getSchemaAdjacencies();
        assertEquals(expectedSchema.size(), schema.size());
        for (int i = 0; i < schema.size(); i++) {
            ArrayList<Pair> row1 = schema.get(i);
            ArrayList<Pair> row2 = expectedSchema.get(i);
            assertEquals(row2.size(), row1.size());
            for (int j = 0; j < row1.size(); j++) {
                Pair pair1 = row1.get(j);
                Pair pair2 = row2.get(j);
                assertEquals(pair2.car(), pair1.car());
                assertEquals(pair2.cdr(), pair1.cdr());
            }
        }
    }
}
