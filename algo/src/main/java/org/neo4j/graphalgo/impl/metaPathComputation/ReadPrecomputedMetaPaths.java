package org.neo4j.graphalgo.impl.metaPathComputation;

import java.io.*;
import java.util.HashMap;
import java.util.regex.Pattern;

public class ReadPrecomputedMetaPaths extends MetaPathComputation {

    public ReadPrecomputedMetaPaths() {

    }

    public ReadPrecomputedMetaPaths.Result readMetaPaths(String filePath)
    {
        HashMap<String, Long> metaPathDict = new HashMap<>();
        try(BufferedReader br = new BufferedReader(new FileReader(filePath))) {
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

        return new ReadPrecomputedMetaPaths.Result(metaPathDict);
    }

    @Override
    public ReadPrecomputedMetaPaths me() { return this; }

    @Override
    public ReadPrecomputedMetaPaths release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        HashMap<String, Long> metaPathsDict;
        public Result(HashMap<String, Long> metaPathsDict) {
            this.metaPathsDict = metaPathsDict;
        }

        @Override
        public String toString() {
            return "Result{}";
        }

        public HashMap<String, Long> getMetaPathsDict() {
            return metaPathsDict;
        }
    }
}
