package org.neo4j.graphalgo.impl;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Has;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.ArrayGraphInterface;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.computeAllMetaPaths.ComputeAllMetaPaths;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FilterMetaPaths extends Algorithm<FilterMetaPaths> {

    public FilterMetaPaths()
    {

    }

    public Result filter(String startLabel, String endLabel)
    {
        LinkedHashMap<String, Integer> metaPathDict = new LinkedHashMap<>();
        try(BufferedReader br = new BufferedReader(new FileReader("Precomputed_MetaPaths.txt"))) {
            String line = br.readLine();

            while (line != null) {
                String[] parts = line.split(Pattern.quote("\t"));
                metaPathDict.put(parts[0], Integer.parseInt(parts[1]));
                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, Integer> filteredDict = new HashMap<>();
        Object[] arrayMetaPaths =  metaPathDict.keySet().toArray();
        int filterIndex = Arrays.binarySearch(arrayMetaPaths, startLabel + " | " + endLabel,
                (a, b) -> metaPathCompare(a.toString(), b.toString()));//TODO: write test for sort

        if(filterIndex < 0) filterIndex = -filterIndex-1;
        if(Integer.parseInt(arrayMetaPaths[filterIndex].toString().split(Pattern.quote(" | "))[0]) == 1 );

        return new Result(0);
    }

    private int metaPathCompare(String a, String b) {
        String[] partsA = a.split(Pattern.quote(" | "));
        String[] partsB = b.split(Pattern.quote(" | "));
        boolean firstLabelEqual = Integer.parseInt(partsA[0]) == Integer.parseInt(partsB[0]);
        boolean lastLabelEqual = Integer.parseInt(partsA[partsA.length - 1]) == Integer.parseInt(partsB[partsB.length - 1]);
        boolean firstLabelSmaller = Integer.parseInt(partsA[0]) < Integer.parseInt(partsB[0]);
        boolean lastLabelSmaller = Integer.parseInt(partsA[partsA.length - 1]) < Integer.parseInt(partsB[partsB.length - 1]);

        return  firstLabelSmaller || (firstLabelEqual && lastLabelSmaller) ? -1 :
                firstLabelEqual && lastLabelEqual ? 0 : 1;
    }

    @Override
    public FilterMetaPaths me() { return this; }

    @Override
    public FilterMetaPaths release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        int finalMetaPaths;
        public Result(int x) {
            this.finalMetaPaths = finalMetaPaths;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public int getFinalMetaPaths() {
            return finalMetaPaths;
        }
    }
}
