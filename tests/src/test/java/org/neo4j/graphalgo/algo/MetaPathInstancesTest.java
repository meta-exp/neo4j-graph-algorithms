package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.graphalgo.walkingProcs.MetaPathInstancesProc;
import org.neo4j.graphalgo.walkingProcs.NodeWalkerProc;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class MetaPathInstancesTest {

    private static final int NODE_COUNT = 50;
    // This rule starts a Neo4j instance
    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(MetaPathInstancesProc.class);
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
        String query = "CREATE (a:Person {name:'Annabella'})\n" +
                "CREATE (b:Person {name:'Christopher'})\n" +
                "CREATE (c:Person {name:'Florian'})\n"+
                "CREATE (d:Bed)\n"+
                "CREATE (e:Pillow {material:'Sheep Wool'})\n"+
                "CREATE (f:Pillow {material:'Goat Wool'})\n"+
                "CREATE (g:Pillow {material:'Foam'})\n"+

                "CREATE" +
                " (a)-[:HAS_BED]->(d),\n" +
                " (b)-[:HAS_BED]->(d),\n" +
                " (c)-[:HAS_BED]->(d),\n" +
                " (d)-[:HAS_PILLOW]->(e),\n" +
                " (d)-[:HAS_PILLOW]->(f),\n" +
                " (d)-[:HAS_PILLOW]->(g)\n";

//        for (int i = 0; i < NODE_COUNT; i++) {
//            query += "CREATE (`" + i + "`:Node {name:'" + 1 + "'})\n" +
//                    "CREATE (`" + i + "`)-[:OF_TYPE {cost:5, blue: 1}]->(a),\n" +
//                    "(b)-[:OF_TYPE {cost:5, blue: 1}]->(`" + i + "`)\n";
//        }

        return query;
    }

    @AfterClass
    public static void tearDown() {
        driver.close();
    }

    @Test
    public void shouldHaveResults() {
        Iterator<Record> results = session.run("CALL metaPathInstances(\"Person%%HAS_BED%%Bed%%HAS_PILLOW%%Pillow\")");

        assertTrue(results.hasNext());
    }

    @Test
    public void shouldHaveRightAmountOfResults() {
        Iterator<Record> results = session.run("CALL metaPathInstances(\"Person%%HAS_BED%%Bed%%HAS_PILLOW%%Pillow\")");

        int count = 0;
        while(results.hasNext()) {
            count++;
            Record record = results.next();
            System.out.print("");
        }

        assertTrue( count == 9);
    }


}