package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.walkingProcs.NodeWalkerProc;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class RandomWalkTest {

    private static final int NODE_COUNT = 50;

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setUp() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(NodeWalkerProc.class);

        db.execute(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT));
    }

    private static String buildDatabaseQuery() {
        return "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Fred {name:'b'})\n" +
                "CREATE (c:Fred {name:'c'})\n" +
                "CREATE (d:Bob {name:'d'})\n" +

                "CREATE" +
                " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                " (c)-[:DIFFERENT {cost:2, blue: 0}]->(b),\n" +
                " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c) " +

             " WITH * UNWIND range(0,$count) AS id CREATE (n:Node {name:''+id})\n" +
                "CREATE (n)-[:OF_TYPE {cost:5, blue: 1}]->(a),\n" +
                    "(b)-[:OF_TYPE {cost:5, blue: 1}]->(n)\n";
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldHaveGivenStartNode() {
        ResourceIterator<Path> result = db.execute("CALL randomWalk(1, 1, 1)").<Path>columnAs("path");

        assertThat((long) 1, equalTo(getStartNodeId(result.next())));
    }

    @Test
    public void shouldHaveResults() {
        Result results = db.execute("CALL randomWalkFromNodeType(1, 5)");

        assertTrue(results.hasNext());
    }

    @Test
    public void shouldHaveSameTypesForStartNodes() {
        // TODO: make this test predictable (i.e. set random seed)
        ResourceIterator<Path> results = db.execute("CALL randomWalkFromNodeType(1, 5, 'Fred')").<Path>columnAs("path");

        while (results.hasNext()) {
            Path record = results.next();

            assertTrue("Nodes should be of type 'Fred'.", getStartNode(record).hasLabel(Label.label("Fred")));
        }
    }

    @Test
    public void shouldHaveStartedFromEveryNode() {
        ResourceIterator<Path> results = db.execute("CALL randomWalkFromAllNodes(1, 1)").<Path>columnAs("path");

        Set<Long> nodeIds = new HashSet<>();
        while (results.hasNext()) {
            Path record = results.next();
            System.out.print(" " + getStartNodeId(record));
            assertFalse("Should not start from any node multiple times.", nodeIds.contains(getStartNodeId(record)));
            nodeIds.add(getStartNodeId(record));
        }
        System.out.println(" << Printed all nodes");

        long numberOfNotes = db.execute("MATCH (n) RETURN COUNT(*) as count;").<Long>columnAs("count").next();
        assertEquals("Should have visited all nodes. Visited " + nodeIds.size() + " of " + numberOfNotes,
                nodeIds.size() , numberOfNotes);
    }

    @Test
    public void shouldNotFail() {
        Result results = db.execute("CALL randomWalk(2, 7, 2)");

        results.next();
        results.next();
        assertTrue("There should be only two results.", !results.hasNext());
    }

    private Node getStartNode(Path path) {
        return path.startNode();
    }

    private long getStartNodeId(Path path) {
        return path.startNode().getId();
    }
}