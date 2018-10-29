package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metapath.getSchema.GetSchema;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.impl.metapath.Pair;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;

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
    public void setupGraph() throws FileNotFoundException {
        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        algo = new GetSchema(graph, null); // TODO
    }

    @Test
    public void testSchema() {
        GetSchema.Result result = algo.compute();
        ArrayList<HashSet<Pair>> expectedSchema = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            expectedSchema.add(new HashSet<>());
        }

        Pair pair = new Pair(0, 0);
        expectedSchema.get(0).add(pair);
        pair = new Pair(1, 0);
        expectedSchema.get(0).add(pair);
        pair = new Pair(2, 0);
        expectedSchema.get(0).add(pair);
        pair = new Pair(2, 1);
        expectedSchema.get(0).add(pair);

        pair = new Pair(0, 0);
        expectedSchema.get(1).add(pair);
        pair = new Pair(2, 0);
        expectedSchema.get(1).add(pair);
        pair = new Pair(2, 1);
        expectedSchema.get(1).add(pair);

        pair = new Pair(0, 0);
        expectedSchema.get(2).add(pair);
        pair = new Pair(0, 1);
        expectedSchema.get(2).add(pair);
        pair = new Pair(1, 0);
        expectedSchema.get(2).add(pair);
        pair = new Pair(1, 1);
        expectedSchema.get(2).add(pair);
        pair = new Pair(2, 1);
        expectedSchema.get(2).add(pair);

        ArrayList<HashSet<Pair>> schema = result.getSchema();
        assertEquals(expectedSchema.size(), schema.size());
        for (int i = 0; i < schema.size(); i++) {
            HashSet<Pair> row1 = schema.get(i);
            HashSet<Pair> row2 = expectedSchema.get(i);
            assertEquals(row2.size(), row1.size());
            for (Pair expectedPair : row2) {
                boolean pairFound = false;
                for (Pair actualPair : row1) {
                    if (expectedPair.equals(actualPair)) {
                        pairFound = true;
                    }
                }
                assertEquals(true, pairFound);
            }
        }

        /*for(HashSet<Pair> neighbours : schema)
        {
            String outputstring = "";
            for(Pair neighbourPair : neighbours)
            {
                outputstring += "(" + neighbourPair.first() + " " + neighbourPair.second() + ") ";
            }
            outputstring += "\n";
            System.out.println(outputstring);
        }*/
    }
}
