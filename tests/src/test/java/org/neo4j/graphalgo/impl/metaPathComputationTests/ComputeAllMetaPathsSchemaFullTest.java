package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFull;
import org.neo4j.graphalgo.impl.metaPathComputation.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import java.util.*;
import static org.junit.Assert.assertEquals;


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

public class ComputeAllMetaPathsSchemaFullTest {

    private static GraphDatabaseAPI api;
    private ComputeAllMetaPathsSchemaFull algo;

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

    }

    //TODO could be a problem if we consider the direction of edges//concerns testPairHashSet.


    @Test
    public void testSchemaFull() throws Exception {

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

        pair = new Pair(0, 0);
        expectedSchema.get(1).add(pair);
        pair = new Pair(2, 0);
        expectedSchema.get(1).add(pair);

        pair = new Pair(0, 0);
        expectedSchema.get(2).add(pair);
        pair = new Pair(1, 0);
        expectedSchema.get(2).add(pair);
        pair = new Pair(2, 0);
        expectedSchema.get(2).add(pair);

        HashMap<Integer, Integer> reversedLabelDictionary = new HashMap<>();
        reversedLabelDictionary.put(0, 0);
        reversedLabelDictionary.put(1, 1);
        reversedLabelDictionary.put(2, 2);

        algo = new ComputeAllMetaPathsSchemaFull(3, expectedSchema, reversedLabelDictionary);
        ComputeAllMetaPathsSchemaFull.Result result = algo.compute();
        HashSet<String> actualMetaPaths = result.getFinalMetaPaths();

        for(String mp : actualMetaPaths)
        {
            System.out.println(mp);
        }

        HashSet<String> expectedMetaPaths = new HashSet<>(Arrays.asList(
                "0|0|0|0|0" ,
                "0|0|0|0|1" ,
                "0|0|0|0|2" ,
                "0|0|0" ,
                "0|0|1" ,
                "0|0|2" ,
                "1|0|0" ,
                "1|0|2" ,
                "2|0|0" ,
                "2|0|1" ,
                "2|0|2" ,
                "0" ,
                "1" ,
                "2" ,
                "0|0|1|0|0" ,
                "0|0|1|0|2" ,
                "0|0|2|0|0" ,
                "0|0|2|0|1" ,
                "0|0|2|0|2" ,
                "1|0|0|0|0" ,
                "1|0|0|0|1" ,
                "1|0|0|0|2" ,
                "1|0|2|0|0" ,
                "1|0|2|0|1" ,
                "1|0|2|0|2" ,
                "2|0|0|0|0" ,
                "2|0|0|0|1" ,
                "2|0|0|0|2" ,
                "2|0|1|0|0" ,
                "2|0|1|0|2" ,
                "2|0|2|0|0" ,
                "2|0|2|0|1" ,
                "2|0|2|0|2"));

        for(String mp : actualMetaPaths)
        {
            assert(expectedMetaPaths.contains(mp));
        }
        //assertEquals(expectedMetaPaths, actualMetaPaths);

        assertEquals(expectedMetaPaths.size(), actualMetaPaths.size());
    }
}
