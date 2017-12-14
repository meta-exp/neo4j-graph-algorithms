package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MetaPath extends Algorithm<MetaPath> {

    private HandyStuff handyStuff;
    private Degrees degrees;
    private int startNodeId;
    private int endNodeId;
    private int randomWalkLength;
    private int numberOfrandomWalks;
    private String [][] metapaths;
    private int nodeHopId;
    private Random random;

    public MetaPath(IdMapping idMapping,
                    HandyStuff handyStuff,
                    Degrees degrees,
                    long startNodeId,
                    long endNodeId,
                    int numberOfRandomWalks,
                    int randomWalkLength){
        this.handyStuff = handyStuff;
        this.degrees = degrees;
        this.startNodeId = idMapping.toMappedNodeId(startNodeId);
        this.nodeHopId = idMapping.toMappedNodeId(startNodeId);
        this.endNodeId = idMapping.toMappedNodeId(endNodeId);
        this.numberOfrandomWalks = numberOfRandomWalks;
        this.randomWalkLength = randomWalkLength;
        this.metapaths = new String[numberOfRandomWalks][randomWalkLength+1];
        this.random = new Random();
    }

    public Result compute() {

        for(int i=0; i < numberOfrandomWalks; i++) {
            nodeHopId = startNodeId;
            metapaths[i][0] = handyStuff.getLabel(nodeHopId);
            for(int j=1; j <= randomWalkLength; j++){
                int degree = degrees.degree(nodeHopId, Direction.OUTGOING);
                if (degree > 0) {
                    int randomEdgeIndex= random.nextInt(degree);
                    nodeHopId = handyStuff.getNodeOnOtherSide(nodeHopId, randomEdgeIndex);
                    // map back to neo4j-ids?
                    metapaths[i][j] = handyStuff.getLabel(nodeHopId);
                }
                else {
                    metapaths[i][j] = "X";
                }
            }
        }

        HashSet<String> finalMetaPaths = new HashSet<>();

        for(int i=0; i < numberOfrandomWalks; i++){
            String strArray[] = Arrays.stream(metapaths[i])
                    .toArray(String[]::new);

            finalMetaPaths.add(String.join(" | ", strArray ) + "\n");
        }

        for (String s:finalMetaPaths
             ) {
            System.out.println(s);
        }

        /*
        for(int i=0; i < numberOfrandomWalks; i++){
            if(containsEndpoint(metapaths[i])){
                for(int j=0; j < randomWalkLength; i++) {
                    System.out.print(Integer.toString(metapaths[i][j]) + " - ");
                    if(metapaths[i][j] == endNodeId){
                        System.out.println();
                        break;
                    }
                }
            }
        }
        */

        return new Result();
    }

    private boolean containsEndpoint(int[] walkInstance){
        for(int i=0; i < walkInstance.length; i++){
            if(walkInstance[i] == endNodeId)
                return true;
        }

        return false;
    }


    public Stream<MetaPath.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new Result());
    }

    @Override
    public  MetaPath me() { return this; }

    @Override
    public MetaPath release() {
        return null;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public Result() {

        }

        @Override
        public String toString() {
            return "Result{}";
        }
    }

}
