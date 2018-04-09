package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.GettingStartedProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.impl.multiTypes.MultiTypes;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class MultiTypeTest {

    private static GraphDatabaseAPI api;
    private static MultiTypes algo;
    private static List<String> labelsBefore;
    private static List<String> labelsAfter;

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
                        "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (b:Type {name:'b'})\n" +
                        "CREATE (c:Type {name:'c'})\n" +

                        "CREATE" +
                        " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                        " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                        " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c), \n" +
                        " (c)-[:DIFFERENT {cost:2, blue: 0}]->(b)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(GettingStartedProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    private static void setupAlgo() throws Exception {

        algo = new MultiTypes( api, "OF_TYPE", "Type", null);

        labelsBefore = listLabels();
        algo.compute();
        labelsAfter = listLabels();
    }

    @Test
    public void testAddLabels() throws Exception {
        assertTrue("There should have been more labels than before.",
                labelsBefore.size() < labelsAfter.size());
    }

    @Test
    public void testHasLabels() throws Exception {
        String[] expectedLabels = {"Node", "Type", "b", "c"};
        String[] actualLabels = labelsAfter.toArray(new String[0]);

        Arrays.sort(actualLabels);
        Arrays.sort(expectedLabels);

        assertArrayEquals("New labels should be added.", actualLabels, expectedLabels);
    }


    @Test
    public void testNodeAHasCorrectLabels() throws Exception {
        String[] labels = {"Node", "b", "c"};
        testCorrectLabels("a", "Node", Arrays.asList(labels));
    }

    @Test
    public void testNodeDCorrectLabels() throws Exception {
        String[] labels = {"Node"};
        testCorrectLabels("d", "Node", Arrays.asList(labels));
    }

    @Test
    public void testTypeHasCorrectLabels() throws Exception {
        String[] labelsB = {"Type", "c"};
        testCorrectLabels("b", "Type", Arrays.asList(labelsB));

        String[] labelsC = {"Type"};
        testCorrectLabels("c", "Type", Arrays.asList(labelsC));
    }

    private void testCorrectLabels(String name, String type, List<String> expectedLabels) {
        try(Transaction transaction = api.beginTx()) {

            Node node = api.findNode(Label.label(type), "name", name);

            for (Label label : node.getLabels()) {
                assertTrue(expectedLabels.contains(label.name()));
            }

            transaction.success();
        }
    }

    private static List<String> listLabels() {
        List<String> labels = new ArrayList<>();
        try(Transaction transaction = api.beginTx()) {
            for (Label label : api.getAllLabels())
                labels.add(label.name());
            transaction.success();
        }
        return labels;
    }
}
