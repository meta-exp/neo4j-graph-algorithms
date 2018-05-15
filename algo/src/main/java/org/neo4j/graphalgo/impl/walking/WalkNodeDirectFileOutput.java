package org.neo4j.graphalgo.impl.walking;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class WalkNodeDirectFileOutput extends AbstractWalkOutput {
    private PrintStream output;
    private int count = 0;

    public WalkNodeDirectFileOutput(String filePath) throws IOException {
        super();
        this.output = new PrintStream(new FileOutputStream(filePath));
    }

    public void endInput(){
        this.output.close();
    }

    public synchronized void addResult(long[][] result) {
        count++;
        this.output.println(arrayToString(result[0]));
    }

    private String arrayToString(long[] array){
        String str = String.valueOf(array[0]);
        for(int i = 1; i < array.length; i++){
            long l = array[i];
            str += " " + String.valueOf(l);
        }
        return str;
    }

    public int numberOfResults(){
        return count;
    }
}