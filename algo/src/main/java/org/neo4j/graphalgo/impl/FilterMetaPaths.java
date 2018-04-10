package org.neo4j.graphalgo.impl;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class FilterMetaPaths extends Algorithm<FilterMetaPaths> {

    private PrintStream out;

    public FilterMetaPaths() throws FileNotFoundException {
        this.out = new PrintStream(new FileOutputStream("Filtered_MetaPaths.txt"));//ends up in root/tests //or in dockerhome
    }

    public Result filter(String startLabel, String endLabel)//TODO: write test for filter
    {
        LinkedHashMap<String, Long> metaPathDict = new LinkedHashMap<>();
        try(BufferedReader br = new BufferedReader(new FileReader("Precomputed_MetaPaths.txt"))) {
            String line = br.readLine();

            while (line != null) {
                String[] parts = line.split(Pattern.quote("\t"));
                metaPathDict.put(parts[0], Long.parseLong(parts[1]));
                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, Long> filteredMetaPathsDict = new HashMap<>();
        Object[] arrayMetaPaths =  metaPathDict.keySet().toArray();
        int filterIndex = Arrays.binarySearch(arrayMetaPaths, startLabel + " | " + endLabel,
                (a, b) -> metaPathCompare(a.toString(), b.toString()));//TODO: write test for sort

        if(filterIndex < 0) filterIndex = -filterIndex-1;
        while(filterIndex >= 0 && metaPathCompare(startLabel + " | " + endLabel, arrayMetaPaths[filterIndex].toString()) == 0)
        {
            filterIndex--;
        }
        filterIndex++;

        while(filterIndex < arrayMetaPaths.length && metaPathCompare(startLabel + " | " + endLabel, arrayMetaPaths[filterIndex].toString()) == 0)
        {
            String metaPath = arrayMetaPaths[filterIndex].toString();
            filteredMetaPathsDict.put(metaPath, metaPathDict.get(metaPath));
            out.println(metaPath + "\t" + metaPathDict.get(metaPath));
            filterIndex++;
        }

        return new Result(filteredMetaPathsDict);
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

        HashMap<String, Long> filteredMetaPathsDict;
        public Result(HashMap<String, Long> filteredMetaPathsDict) {
            this.filteredMetaPathsDict = filteredMetaPathsDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashMap<String, Long> getFilteredMetaPathsDict() {
            return filteredMetaPathsDict;
        }
    }
}
