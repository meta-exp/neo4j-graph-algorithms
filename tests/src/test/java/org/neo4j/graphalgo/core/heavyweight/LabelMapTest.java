package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.metaPathComputationProcs.GettingStartedProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class LabelMapTest {

    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (a:SNP {name:\"a\"})\n" +
                        "CREATE (b:PHN {name:\"b\"})\n" +
                        "CREATE (c:SNP {name:\"c\"})\n" +
                        "CREATE (d:PHN {name:\"d\"})\n" +
                        "CREATE (e:GEN {name:\"e\"})\n" +
                        "CREATE (f:SNP {name:\"f\"})\n" +
                        "CREATE (g:PHN {name:\"g\"})\n" +
                        "CREATE (h:GEN {name:\"h\"})\n" +
                        "CREATE (i:SNP {name:\"i\"})\n" +
                        "CREATE (j:GEN {name:\"j\"})\n" +
                        "CREATE (k:SNP {name:\"k\"})\n" +
                        "CREATE (l:GEN {name:\"l\"})\n" +
                        "CREATE (m:PHN {name:\"m\"})\n" +
                        "CREATE (n:SNP {name:\"n\"})\n" +
                        "CREATE (o:GEN {name:\"o\"})\n" +
                        "CREATE (p:PHN {name:\"p\"})\n" +
                        "CREATE (q:GEN {name:\"q\"})\n" +
                        "CREATE (r:SNP {name:\"r\"})\n" +
                        "CREATE (s:GEN {name:\"s\"})\n" +
                        "CREATE (t:PHN {name:\"t\"})\n" +
                        "CREATE\n" +
                        "  (a)-[:TYPE1]->(t),\n" +
                        "  (a)-[:TYPE1]->(c),\n" +
                        "  (a)-[:TYPE1]->(b),\n" +
                        "  (a)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(s),\n" +
                        "  (b)-[:TYPE1]->(t),\n" +
                        "  (c)-[:TYPE1]->(s),\n" +
                        "  (c)-[:TYPE1]->(h),\n" +
                        "  (l)-[:TYPE1]->(b),\n" +
                        "  (h)-[:TYPE1]->(l),\n" +
                        "  (c)-[:TYPE1]->(b),\n" +
                        "  (d)-[:TYPE1]->(a),\n" +
                        "  (d)-[:TYPE1]->(b),\n" +
                        "  (e)-[:TYPE1]->(b),\n" +
                        "  (e)-[:TYPE1]->(d),\n" +
                        "  (e)-[:TYPE1]->(f),\n" +
                        "  (f)-[:TYPE1]->(b),\n" +
                        "  (f)-[:TYPE1]->(c),\n" +
                        "  (g)-[:TYPE1]->(b),\n" +
                        "  (g)-[:TYPE1]->(e),\n" +
                        "  (h)-[:TYPE1]->(b),\n" +
                        "  (h)-[:TYPE1]->(e),\n" +
                        "  (i)-[:TYPE1]->(b),\n" +
                        "  (i)-[:TYPE1]->(e),\n" +
                        "  (i)-[:TYPE1]->(t),\n" +
                        "  (j)-[:TYPE1]->(s),\n" +
                        "  (j)-[:TYPE1]->(e),\n" +
                        "  (t)-[:TYPE1]->(o),\n" +
                        "  (k)-[:TYPE1]->(e),\n" +
                        "  (m)-[:TYPE2]->(n),\n" +
                        "  (m)-[:TYPE2]->(q),\n" +
                        "  (m)-[:TYPE2]->(p),\n" +
                        "  (m)-[:TYPE2]->(o),\n" +
                        "  (n)-[:TYPE2]->(k),\n" +
                        "  (k)-[:TYPE2]->(r),\n" +
                        "  (s)-[:TYPE1]->(m)\n";

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
    public void testLabelMapNotNull(){
        final HeavyGraph graphWithLabelMap;

        graphWithLabelMap = (HeavyGraph) new GraphLoader(api)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        assert -1 != graphWithLabelMap.getLabel(1);
    }

    @Test
    public void testLabelMapNull(){
        final HeavyGraph graphWithoutLabelMap;
        graphWithoutLabelMap = (HeavyGraph) new GraphLoader(api)
                .withLabelAsProperty(false)
                .load(HeavyGraphFactory.class);
        assert -1 == graphWithoutLabelMap.getLabel(1);
    }

    @Test
    public void testDefaultGraphLoader(){
        final HeavyGraph graphWithoutLabelMap;
        graphWithoutLabelMap = (HeavyGraph) new GraphLoader(api)
                .load(HeavyGraphFactory.class);
        assert -1 == graphWithoutLabelMap.getLabel(1);
    }

    @Test
    public void testLabelMapForKey(){
        final HeavyGraph graphWithLabelMap;

        graphWithLabelMap = (HeavyGraph) new GraphLoader(api)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        assert(1 == graphWithLabelMap.getLabel(1));
    }

    @Test
    public void testGetAllLabels() {
        final HeavyGraph graphWithLabelMap;
        graphWithLabelMap = (HeavyGraph) new GraphLoader(api)
                .withLabelAsProperty(true)
                .load(HeavyGraphFactory.class);

        Collection<Integer> allLabels = graphWithLabelMap.getAllLabels();
        assertEquals(3, allLabels.size());
    }
}

