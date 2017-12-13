package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public class MetaPath extends Algorithm<MetaPath> implements RelationshipConsumer {

    private IdMapping idMapping;
    private NodeIterator nodeIterator;
    private RelationshipIterator relationshipIterator;
    private Degrees degrees;
    private int startNodeId;
    private int endNodeId;
    private int randomWalkLength;
    private int numberOfrandomWalks;
    private int nodeCount;
    private int [][] metapaths;
    private int edgeCounter = 0;
    private int randomIndex;
    private int nextNodeHopId;
    private Random random;

    public MetaPath(IdMapping idMapping,
                    NodeIterator nodeIterator,
                    RelationshipIterator relationshipIterator,
                    Degrees degrees,
                    int startNodeId,
                    int endNodeId,
                    int numberOfRandomWalks,
                    int randomWalkLength){
        this.idMapping = idMapping;
        this.nodeIterator = nodeIterator;
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.numberOfrandomWalks = numberOfRandomWalks;
        this.randomWalkLength = randomWalkLength;
        this.metapaths = new int[numberOfRandomWalks][randomWalkLength];
        this.nextNodeHopId = startNodeId;
        this.random = new Random();
    }

    public Result compute() {
        for(int j=0; j < numberOfrandomWalks; j++ ) {
            for(int i = 0; i < randomWalkLength; i++) {
                int degree = degrees.degree(nextNodeHopId, Direction.OUTGOING);
                randomIndex = random.nextInt(degree);
                relationshipIterator.forEachOutgoing(nextNodeHopId, this);
                metapaths[j][i] = nextNodeHopId;
            }
        }

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

        return new Result();
    }

    private boolean containsEndpoint(int[] walkInstance){
        for(int i=0; i < walkInstance.length; i++){
            if(walkInstance[i] == endNodeId)
                return true;
        }

        return false;
    }

    @Override
    public boolean accept(
            int sourceNodeId,
            int targetNodeId,
            long relationId) {

        if(edgeCounter == randomIndex){
            nextNodeHopId = targetNodeId;
            return false;
        }

        edgeCounter ++;

        return true;
    }

    public Stream<MetaPath.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new Result());
    }

    @Override
    public  MetaPath me() { return this; }

    @Override
    public MetaPath release() {
        idMapping = null;
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
