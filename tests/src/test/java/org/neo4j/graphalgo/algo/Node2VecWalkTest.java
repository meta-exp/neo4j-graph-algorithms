package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.graphalgo.walkingProcs.NodeWalkerProc;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class Node2VecWalkTest {

    private static final int NODE_COUNT = 50;
    // This rule starts a Neo4j instance
    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(NodeWalkerProc.class);
    private static Session session;
    private static Driver driver;

    @BeforeClass
    public static void setUp() {
        driver = GraphDatabase
                .driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
        session = driver.session();
        session.run(buildDatabaseQuery());
    }

    private static String buildDatabaseQuery() {
        String query = "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Fred {name:'b'})\n" +
                "CREATE (c:Fred {name:'c'})\n" +
                "CREATE (d:Bob {name:'d'})\n" +

                "CREATE" +
                " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                " (c)-[:DIFFERENT {cost:2, blue: 0}]->(b),\n" +
                " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c)";

        for (int i = 0; i < NODE_COUNT; i++) {
            query += "CREATE (`" + i + "`:Node {name:'" + 1 + "'})\n" +
                    "CREATE (`" + i + "`)-[:OF_TYPE {cost:5, blue: 1}]->(a),\n" +
                    "(b)-[:OF_TYPE {cost:5, blue: 1}]->(`" + i + "`)\n";
        }

        return query;
    }

    @AfterClass
    public static void tearDown() {
        driver.close();
    }

    @Test
    public void shouldHaveGivenStartNode() {
        Record result = session.run("CALL node2vecWalk(1, 1, 1, 1, 1)").single();

        assertThat((long) 1, equalTo(getStartNodeId(result)));
    }

    @Test
    public void shouldHaveResults() {
        Iterator<Record> results = session.run("CALL node2vecWalkFromAllNodes(1, 5, 1, 1)");

        assertTrue(results.hasNext());
    }

    @Test
    public void shouldHaveStartedFromEveryNode() {
        Iterator<Record> results = session.run("CALL node2vecWalkFromAllNodes(1, 1, 1, 1)");

        List<Long> nodeIds = new ArrayList<>();
        while (results.hasNext()) {
            Record record = results.next();
            System.out.print(" " + getStartNodeId(record));
            assertFalse("Should not start from any node multiple times.", nodeIds.contains(getStartNodeId(record)));
            nodeIds.add(getStartNodeId(record));
        }
        System.out.println(" << Printed all nodes");

        int numberOfNotes = session.run("MATCH (n) RETURN COUNT(*) as count;").single().get("count").asInt();
        assertTrue("Should have visited all nodes. Visited " + nodeIds.size() + " of " + numberOfNotes,
                nodeIds.size() == numberOfNotes);
    }

    @Test
    public void shouldNotFail() {
        Iterator<Record> results = session.run("CALL node2vecWalk(2, 7, 2, 1, 1)");

        results.next();
        results.next();
        assertTrue("There should be only two results.", !results.hasNext());
    }

    private Node getStartNode(Record randomWalkRecord) {
        return getPath(randomWalkRecord).start();
    }

    private long getStartNodeId(Record randomWalkRecord) {
        return getStartNode(randomWalkRecord).id();
    }

    private Path getPath(Record randomWalkRecord) {
        return randomWalkRecord.get("path").asPath();
    }


}