package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.impl.metaPathComputation.GraphReducer;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertArrayEquals;

public class GraphReducerTest {
    private static GraphDatabaseAPI api;
    private static List<String> labelsAfter;
    private static List<String> edgeLabelsAfter;

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

        String[] goodNodeTypes = {"Type"};
        String[] goodEdgeTypes = {"DIFFERENT"};

        GraphReducer algo = new GraphReducer(api, null, goodNodeTypes, goodEdgeTypes);
        algo.compute();
        labelsAfter = listLabels();
        edgeLabelsAfter = listEdgeLabels();
    }

    private static List<String> listEdgeLabels() throws Exception {
        List<String> edgeLabels = new ArrayList<>();
        try (Transaction transaction = api.beginTx()) {
            for (RelationshipType relType : api.getAllRelationshipTypes()) {
                edgeLabels.add(relType.name());
            }
            transaction.success();
        }
        return edgeLabels;
    }

    @Test
    public void testNodeLabelStillExists() throws Exception {
        String expectedLabel = "Type";

        assert(labelsAfter.contains(expectedLabel));
    }

    @Test
    public void testHasEdgeLabels() throws Exception {
        String expectedLabel = "DIFFERENT";

        assert(edgeLabelsAfter.contains(expectedLabel));
    }


    @Test
    public void testNodeCExists() throws Exception {
        testNodeExists("c", "Type");
    }

    @Test
    public void testNodeBExists() throws Exception {
        testNodeExists("b", "Type");
    }

    @Test
    public void testNodeBNotExists() throws Exception {
        testNodeNotExists("A", "Node");
    }

    @Test
    public void testNodeDNotExists() throws Exception {
        testNodeNotExists("d", "Node");
    }

    private void testNodeExists(String name, String type) throws Exception {
        try(Transaction transaction = api.beginTx()) {
            Node node = api.findNode(Label.label(type), "name", name);
            assert(node != null);

            transaction.success();
        }
    }

    private void testNodeNotExists(String name, String type) throws Exception {
        try(Transaction transaction = api.beginTx()) {
            Node node = api.findNode(Label.label(type), "name", name);
            assert(node == null);

            transaction.success();
        }
    }

    private static List<String> listLabels() throws Exception {
        List<String> labels = new ArrayList<>();
        try(Transaction transaction = api.beginTx()) {
            for (Label label : api.getAllLabels())
                labels.add(label.name());
            transaction.success();
        }
        return labels;
    }
}
