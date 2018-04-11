package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphalgo.impl.metaPathComputation.ReadPrecomputedMetaPaths;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ReadPrecomputedMetaPathsTest {
    private ReadPrecomputedMetaPaths algo;
    String filePath;

    @Before
    public void prepareFile() throws IOException{
        filePath = "TEMP_ReadPrecomputedMetaPathsTest.txt";
        PrintStream out = new PrintStream(new FileOutputStream(filePath));
        String[] fileContent = {"0\t4", "1\t2", "2\t2", "0 | 0 | 0\t2", "0 | 0 | 1\t2", "0 | 0 | 2\t3", "0 | 1 | 0\t4", "0 | 1 | 2\t4", "0 | 2 | 0\t13", "0 | 2 | 1\t7", "0 | 2 | 2\t5",
                "0 | 1\t2", "0 | 2\t5", "0 | 0\t2", "1 | 0\t2", "1 | 2\t3", "2 | 0\t5", "2 | 1\t3", "2 | 2\t2"};
        for (String element : fileContent) {
            out.println(element);
        }
        algo = new ReadPrecomputedMetaPaths();
    }

    @Test
    public void testReadPrecomputedMetaPaths() {
        HashMap<String, Long> result = algo.readMetaPaths(filePath).getMetaPathsDict();
        Set<String> allMetaPaths = result.keySet();
        Collection<Long> allCounts = result.values();

        Set<String> expectedMetaPaths = new HashSet<>(Arrays.asList("0", "1", "2", "0 | 0 | 0", "0 | 0 | 1", "0 | 0 | 2", "0 | 1 | 0", "0 | 1 | 2", "0 | 2 | 0", "0 | 2 | 1", "0 | 2 | 2",
                "0 | 1", "0 | 2", "0 | 0", "1 | 0", "1 | 2", "2 | 0", "2 | 1", "2 | 2"));

        Collection<Long> expectedCounts = new HashSet<>(Arrays.asList(4L, 2L, 2L, 2L, 2L, 3L, 4L, 4L, 13L, 7L, 5L, 2L, 5L, 2L, 2L, 3L, 5L, 3L, 2L));

        for (String expectedMetaPath : expectedMetaPaths) {
            System.out.println("expected: " + expectedMetaPath);
            assert(allMetaPaths.contains(expectedMetaPath));
        }
        for (long expectedCount : expectedCounts) {
            System.out.println("expected: " + expectedCount);
            assert(allCounts.contains(expectedCount));
        }

        assertEquals(19, allMetaPaths.size());
    }

}
