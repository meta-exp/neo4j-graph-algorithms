package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.GettingStartedProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.multiTypes.MultiTypes;
import org.neo4j.graphdb.Direction;
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
                        "CREATE (b:Type {name:'b'})\n" +
                        "CREATE (c:Type {name:'c'})\n" +

                        "CREATE" +
                        " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                        " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                        " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c)";

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
        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .withDirection(Direction.OUTGOING)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);



        algo = new MultiTypes(graph, api, "OF_TYPE", "Type");

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
    public void testNodeHasLabels() throws Exception {
        try(Transaction transaction = api.beginTx()) {
            Node a = api.findNode(Label.label("Node"), "name", "a");
            assertTrue(a.hasLabel(Label.label("b"))
                    && a.hasLabel(Label.label("c"))
                    && a.hasLabel(Label.label("Node")));

            transaction.success();
        }
    }

    @Test
    public void testTypeHasLabels() throws Exception {
        try(Transaction transaction = api.beginTx()) {

            Node b = api.findNode(Label.label("Type"), "name", "b");
            assertTrue(b.hasLabel(Label.label("c"))
                    && b.hasLabel(Label.label("Type")));

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
