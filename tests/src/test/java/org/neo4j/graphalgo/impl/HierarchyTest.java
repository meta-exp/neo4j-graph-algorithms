package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.MultiTypesProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.impl.multiTypes.Hierarchy;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class HierarchyTest {

    private static GraphDatabaseAPI api;
    private static Hierarchy algo;
    private static List<String> labelsBefore;
    private static List<String> labelsAfter;

    private static final int DEPTH = 1;

    @BeforeClass
    public static void setup() throws Exception {
        setupData();
        setupAlgo();
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    private static void setupData() throws Exception {
        final String cypher =
                        "CREATE (a:Entity {name:'A', id: 'A'})\n" +
                        "CREATE (b:Entity {name:'B', id: 'B'})\n" +
                        "CREATE (b1:Entity {name:'', id: 'B1'})\n" +
                        "CREATE (c:Entity {name:'C', id: 'C'})\n" +
                        "CREATE (d:Entity {name:'D', id: 'D'})\n" +

                        "CREATE" +
                        " (b)-[:OF_TYPE]->(a),\n" +
                        " (b1)-[:OF_TYPE]->(a),\n" +
                        " (c)-[:OF_TYPE]->(a),\n" +
                        " (d)-[:OF_TYPE]->(b), \n" +
                        " (d)-[:OF_TYPE]->(c)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(MultiTypesProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    private static void setupAlgo() throws Exception {

        algo = new Hierarchy(api, "OF_TYPE", "name", "Class", null);
        try (Transaction transaction = api.beginTx()) {
            Node node = api.findNode(Label.label("Entity"), "name", "A");
            algo.compute(node.getId(), DEPTH);
            transaction.success();
        }
    }

    @Test
    public void testNodeAHasCorrectLabels() throws Exception {
        String[] labels = {"Entity", "A", "Class"};
        testCorrectLabels("A", Arrays.asList(labels));
    }

    @Test
    public void testNodeBHasCorrectLabels() throws Exception {
        String[] labels = {"Entity", "A", "B", "Class"};
        testCorrectLabels("B", Arrays.asList(labels));
    }

    @Test
    public void testNodeShouldNotHaveEmptyLabels() throws Exception {
        String[] labels = {"Entity", "A", "Class"};
        testCorrectLabels("B1", Arrays.asList(labels));
    }

    @Test
    public void testNodeCHasCorrectLabels() throws Exception {
        String[] labels = {"Entity", "A", "C", "Class"};
        testCorrectLabels("C", Arrays.asList(labels));
    }

    @Test
    public void testNodeDHasCorrectLabels() throws Exception {
        String[] labels = {"Entity", "A", "B", "C", "Class"};
        testCorrectLabels("D", Arrays.asList(labels));
    }

    private void testCorrectLabels(String name, List<String> expectedLabels) {
        try (Transaction transaction = api.beginTx()) {

            Node node = api.findNode(Label.label("Entity"), "id", name);

            for (Label label : node.getLabels()) {
                assertTrue("Didn't expect label " + label.name(), expectedLabels.contains(label.name()));
            }

            transaction.success();
        }
    }
}
