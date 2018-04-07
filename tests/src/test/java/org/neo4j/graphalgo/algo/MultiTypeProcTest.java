package org.neo4j.graphalgo.algo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.MultiTypesProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

public class MultiTypeProcTest {

    private GraphDatabaseAPI db;

    private static final String DB_CYPHER =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Type {name:'b'})\n" +
                    "CREATE (c:Type {name:'c'})\n" +

                    "CREATE" +
                    " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                    " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                    " (c)-[:DIFFERENT {cost:2, blue: 0}]->(b),\n" +
                    " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c)";

    @After
    public void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }


    @Before
    public void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(MultiTypesProc.class);
    }

    @Test
    public void testMultiTypesProcCall() throws Exception {
        runQuery(
                "CALL algo.multiTypes('OF_TYPE', 'Type') YIELD success, executionTime",
                row -> assertTrue(row.getBoolean("success")));
    }

    @Test
    public void testMultiTypesSingleNodeProcCall() throws Exception {
        runQuery(
                "MATCH (n {name: 'c'})\n" +
                        "CALL algo.multiTypesSingleNode(ID(n), 'OF_TYPE', 'Type') YIELD success, executionTime\n" +
                        "RETURN success",
                row -> assertTrue(row.getBoolean("success")));
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
