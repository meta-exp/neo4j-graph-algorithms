package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.*;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsSchemaFull;
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

    }

    @Ignore//TODO could be a problem if we consider the direction of edges//comment still relevant?
    @Test
    public void testPairHashSet() {
        org.neo4j.graphdb.Result queryResult;
        final String cypher2 = "CALL algo.computeAllMetaPathsSchemaFull('3');";
        try (Transaction tx = api.beginTx()) {
            queryResult = api.execute(cypher2);
            tx.success();
        }

        Map<String, Object> row = queryResult.next();
        List<String> actualMetaPaths =  (List<String>) row.get("metaPaths");

        ArrayList<String> expectedMetaPaths = new ArrayList<>(Arrays.asList(
                "0 | 0 | 0 | 0 | 0" ,
                "0 | 0 | 0 | 0 | 1" ,
                "0 | 0 | 0 | 0 | 2" ,
                "0 | 0 | 1 | 0 | 0" ,
                "0 | 0 | 1 | 0 | 2" ,
                "0 | 0 | 2 | 0 | 0" ,
                "0 | 0 | 2 | 0 | 1" ,
                "0 | 0 | 2 | 0 | 2" ,
                "1 | 0 | 0 | 0 | 0" ,
                "1 | 0 | 0 | 0 | 1" ,
                "1 | 0 | 0 | 0 | 2" ,
                "1 | 0 | 2 | 0 | 0" ,
                "1 | 0 | 2 | 0 | 1" ,
                "1 | 0 | 2 | 0 | 2" ,
                "2 | 0 | 0 | 0 | 0" ,
                "2 | 0 | 0 | 0 | 1" ,
                "2 | 0 | 0 | 0 | 2" ,
                "2 | 0 | 1 | 0 | 0" ,
                "2 | 0 | 1 | 0 | 2" ,
                "2 | 0 | 2 | 0 | 0" ,
                "2 | 0 | 2 | 0 | 1" ,
                "2 | 0 | 2 | 0 | 2"));

        for(String mp : actualMetaPaths)
        {
            assert(expectedMetaPaths.contains(mp));
        }

        assertEquals(expectedMetaPaths.size(), actualMetaPaths.size());
    }
}
