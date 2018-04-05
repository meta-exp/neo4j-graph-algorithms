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
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertTrue;

public class MultiTypeTest {

    private static GraphDatabaseAPI api;
    private MultiTypes algo;

    @BeforeClass
    public static void setup() throws Exception {
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

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void testConversion() throws Exception {
        final HeavyGraph graph;

        graph = (HeavyGraph) new GraphLoader(api)
                .withDirection(Direction.OUTGOING)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);



        algo = new MultiTypes(graph, api, "OF_TYPE", "Type");

        int i = 0;
        int j = 0;

        // TODO: Clean up the test
        try(Transaction transaction = api.beginTx()) {
            for (Object o : api.getAllLabels())
                i++;
            transaction.success();
        }

        algo.compute();

        try(Transaction transaction = api.beginTx()) {
            for (Label o : api.getAllLabels()) {
                System.out.println(o.name());
                j++;
            }
            transaction.success();
        }

        assertTrue("There should have been more labels than before.", j > i);
    }
}
