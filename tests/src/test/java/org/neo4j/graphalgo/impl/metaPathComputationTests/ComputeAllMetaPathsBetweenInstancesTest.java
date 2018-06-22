package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeAllMetaPathsBetweenInstances;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ComputeAllMetaPathsBetweenInstancesTest {

    private static GraphDatabaseAPI api;
    private ComputeAllMetaPathsBetweenInstances algo;
    private HeavyGraph graph;
    private Log testLog = new Log() {
        @Override public boolean isDebugEnabled() {
            return false;
        }

        @Override public Logger debugLogger() {
            return null;
        }

        @Override public void debug(String s) {
            System.out.println("DEBUG: " + s);
        }

        @Override public void debug(String s, Throwable throwable) {

        }

        @Override public void debug(String s, Object... objects) {

        }

        @Override public Logger infoLogger() {
            return null;
        }

        @Override public void info(String s) {
            System.out.println("INFO: " + s);
        }

        @Override public void info(String s, Throwable throwable) {

        }

        @Override public void info(String s, Object... objects) {

        }

        @Override public Logger warnLogger() {
            return null;
        }

        @Override public void warn(String s) {
            System.out.println("WARN: " + s);
        }

        @Override public void warn(String s, Throwable throwable) {

        }

        @Override public void warn(String s, Object... objects) {

        }

        @Override public Logger errorLogger() {
            return null;
        }

        @Override public void error(String s) {
            System.out.println("ERROR: " + s);
        }

        @Override public void error(String s, Throwable throwable) {

        }

        @Override public void error(String s, Object... objects) {

        }

        @Override public void bulk(Consumer<Log> consumer) {

        }
    };

    @Before
    public void setup() throws KernelException{

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(GettingStartedProc.class);
    }

    private static void run_query(String cypher) {
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @After
    public void shutdownGraph() throws Exception {
        api.shutdown();
        FileUtils.deleteDirectory(new File("/tmp/between_instances/"));
    }

    @Test
    public void testCreationOfFile() {
        String cypher = "CREATE (a:A {name:\"a\"})\n" +
                        "CREATE (b:B {name:\"b\"})\n" +
                        "CREATE (c:C {name:\"c\"})\n" +
                        "CREATE\n" +
                        "  (a)-[:TYPE1]->(b)";
        run_query(cypher);

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        int length = 2;
        algo = new ComputeAllMetaPathsBetweenInstances(graph, length, testLog);
        algo.compute();

        runQuery("MATCH (n1 {name: 'a'}), (n2 {name: 'b'}) RETURN ID(n1) as id_n1, ID(n2) as id_n2", row -> assertTrue(
                (new File("/tmp/between_instances/MetaPaths-" + length + "-0.0_" + row.getNumber("id_n1").longValue() + "_" + row.getNumber("id_n2").longValue() + ".txt").exists())));
        runQuery("MATCH (n1 {name: 'a'}), (n2 {name: 'c'}) RETURN ID(n1) as id_n1, ID(n2) as id_n2", row -> assertFalse(
                (new File("/tmp/between_instances/MetaPaths-" + length + "-0.0_" + row.getNumber("id_n1").longValue() + "_" + row.getNumber("id_n2").longValue() + ".txt").exists())));

    }

    @Test
    public void testComputationMetaPath() {
        String cypher = "CREATE (a:A {name:\"a\"})\n" +
                "CREATE (b:B {name:\"b\"})\n" +
                "CREATE\n" +
                "  (a)-[:TYPE1]->(b)";
        run_query(cypher);

        graph = (HeavyGraph) new GraphLoader(api)
                .asUndirected(true)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        int length = 3;
        algo = new ComputeAllMetaPathsBetweenInstances(graph, length, testLog);
        algo.compute();

        String metapath = "0|0|0";

        runQuery("MATCH (n1 {name: 'a'}), (n2 {name: 'b'}) RETURN ID(n1) as id_n1, ID(n2) as id_n2", row -> {
            Path file = FileSystems.getDefault()
                    .getPath("/tmp/between_instances", "MetaPaths-" + length + "-0.0_" + row.getNumber("id_n1").longValue() + "_" + row.getNumber("id_n2").longValue() + ".txt");
            try {
                try (BufferedReader reader = Files.newBufferedReader(file, Charset.forName("US-ASCII"))) {
					String line = null;
					while ((line = reader.readLine()) != null) {
                        assertEquals(metapath, line);
					}
				}
            } catch (IOException e) {
                e.printStackTrace();
                fail("Exception thrown");
            }
        });
    }

    private void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        try (Result result = api.execute(query)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

}
