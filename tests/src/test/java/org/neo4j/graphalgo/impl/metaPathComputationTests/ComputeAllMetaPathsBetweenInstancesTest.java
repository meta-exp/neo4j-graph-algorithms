package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenInstances;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ComputeAllMetaPathsBetweenInstancesTest {

    private static GraphDatabaseAPI api;
    private ComputeAllMetaPathsBetweenInstances algo;
    private HeavyGraph graph;

    @BeforeClass
    public static void setup() throws KernelException{

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(GettingStartedProc.class);
    }

    private static void run_query(String cypher) {
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void testCreationOfFile() {
        String cypher = "CREATE (a:A {name:\"a\"})\n" +
                        "CREATE (b:B {name:\"b\"})\n" +
                        "CREATE (c:C {name:\"c\"})\n" +
                        "CREATE\n" +
                        "  (a)-[:TYPE1]->(b)";
        run_query(cypher);

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        algo = new ComputeAllMetaPathsBetweenInstances(graph, 2);
        algo.compute();

        runQuery("MATCH (n1 {name: 'a'}), (n2 {name: 'b'}) RETURN ID(n1) as id_n1, ID(n2) as id_n2",
                row -> assertTrue((new File("between_instances/MetaPaths_" + row.getNumber("id_n1").longValue() + "_" + row.getNumber("id_n2").longValue() + ".txt").exists())));
        runQuery("MATCH (n1 {name: 'a'}), (n2 {name: 'c'}) RETURN ID(n1) as id_n1, ID(n2) as id_n2",
                row -> assertFalse((new File("between_instances/MetaPaths_" + row.getNumber("id_n1").longValue() + "_" + row.getNumber("id_n2").longValue() + ".txt").exists())));

    }

    private void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        try (Result result = api.execute(query)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

}
