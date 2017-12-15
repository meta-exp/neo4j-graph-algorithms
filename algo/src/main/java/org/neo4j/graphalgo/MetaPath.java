package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MetaPath extends Algorithm<MetaPath> {

    private HandyStuff handyStuff;
    private Degrees degrees;
    private IdMap mapping;
    private HashSet<Integer> startNodeIds;
    private HashSet<Integer> endNodeIds;
    private int randomWalkLength;
    private int numberOfrandomWalks;
    private ArrayList<ArrayList<String>> metapaths;
    private Random random;

    public MetaPath(IdMapping idMapping,
                    HandyStuff handyStuff,
                    Degrees degrees,
                    HashSet<Long> startNodeIds,
                    HashSet<Long> endNodeIds,
                    int numberOfRandomWalks,
                    int randomWalkLength){

        this.startNodeIds = new HashSet<>();
        this.endNodeIds = new HashSet<>();
        convertIds(idMapping, startNodeIds, this.startNodeIds);
        convertIds(idMapping, endNodeIds, this.endNodeIds);
        this.handyStuff = handyStuff;
        this.degrees = degrees;
        this.numberOfrandomWalks = numberOfRandomWalks;
        this.randomWalkLength = randomWalkLength;
        this.metapaths = new ArrayList<>();
        this.random = new Random();
    }

    private void convertIds(IdMapping idMapping, HashSet<Long> incomingIds, HashSet<Integer> convertedIds){
        for(long l : incomingIds){
          convertedIds.add(idMapping.toMappedNodeId(l));
        }
    }

    public Result compute() {

        for (int nodeId : startNodeIds) {
            computeMetapathFromNode(nodeId);
        }

        HashSet<String> finalMetaPaths = new HashSet<>();

        for(ArrayList<String> metapath :metapaths){
            finalMetaPaths.add(String.join(" | ", metapath ) + "\n");
        }

        for (String s:finalMetaPaths
             ) {
            System.out.println(s);
        }

        return new Result();
    }

    private void computeMetapathFromNode(int startNodeId){
        for(int i=0; i < numberOfrandomWalks; i++) {
            int nodeHopId = startNodeId;
            ArrayList<String> metapath = new ArrayList<>();
            metapath.add(handyStuff.getLabel(nodeHopId));
            for(int j=1; j <= randomWalkLength; j++){
                int degree = degrees.degree(nodeHopId, Direction.OUTGOING);
                if (endNodeIds.contains(nodeHopId)){
                    metapaths.add(metapath);
                    break;
                }
                else if (degree <= 0) {
                    break;
                }
                else {
                    int randomEdgeIndex= random.nextInt(degree);
                    nodeHopId = handyStuff.getNodeOnOtherSide(nodeHopId, randomEdgeIndex);
                    metapath.add(handyStuff.getLabel(nodeHopId));
                }
            }
        }
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
