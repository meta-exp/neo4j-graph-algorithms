package org.neo4j.graphalgo.algo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.HierarchyProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HierarchyProcTest {

    private GraphDatabaseAPI db;

    private static final String CYPHER_PATH = "resources/wikidata_subgraph_small.cypher";

    private static void create_graph(GraphDatabaseAPI db) throws IOException {
            try (Stream<String> stream = Files.lines(Paths.get(CYPHER_PATH))) {
                stream.forEach(line -> {
                    try (Transaction tx = db.beginTx()) {
                        db.execute(line).close();
                        tx.success();
                    }
                });
            }
    }

    @After
    public void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }


    @Before
    public void setup() throws KernelException, IOException {
        db = TestDatabaseCreator.createTestDatabase();
        create_graph(db);

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(HierarchyProc.class);
    }

    @Test
    public void testMultiTypesProcCall() {
        runQuery(
              "MATCH (n) WHERE n.label='entity' " +
                    "CALL algo.hierarchy(ID(n), 3, 'SUBCLASS_OF', 'label', 'CLASS') YIELD success, executionTime " +
                    "RETURN executionTime",
                row -> assertNotNull(row.getNumber("executionTime")));

        runQuery("MATCH (m) WITH COUNT(m) as total " +
                        "MATCH (n:CLASS) WITH COUNT(n) as classes, total as total " +
                        "RETURN total, classes",
                row -> assertEquals(row.getNumber("classes"), row.getNumber("total")));
    }

    @Test
    public void testMultiTypesSingleNodeProcCall() {
        runQuery(
                "MATCH (n) WHERE n.label='entity' " +
                        "CALL algo.hierarchySingleNode(ID(n), 'SUBCLASS_OF', 'label', 'CLASS') YIELD success, executionTime " +
                        "RETURN executionTime",
                row -> assertNotNull(row.getNumber("executionTime")));

        runQuery("MATCH (m)<-[:SUBCLASS_OF]-(n) WHERE m.label='entity' WITH COUNT(m) as total " +
                        "MATCH (m)<-[:SUBCLASS_OF]-(n:CLASS) WHERE m.label='entity' WITH COUNT(n) as classes, total as total " +
                        "RETURN total, classes",
                row -> assertEquals(row.getNumber("classes"), row.getNumber("total")));
    }

    private void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }
}
