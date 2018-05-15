package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.NodeWalkerProc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class WalkDatabaseOutput extends AbstractWalkOutput {
    public ArrayList<long[]> pathIds = new ArrayList<>();
    private Iterator<long[]> pathIterator;
    private GraphDatabaseService db;
    private Log log;

    public WalkDatabaseOutput(GraphDatabaseService db, Log log) {
        super();
        this.db = db;
        this.log = log;
    }

    public void endInput(){
        pathIterator = pathIds.iterator();
    }

    public synchronized void addResult(long[] result) {
        pathIds.add(result);
    }

    public Stream<NodeWalkerProc.WalkResult> getStream() {
        return Stream.generate(
                () -> new NodeWalkerProc.WalkResult(convertIdsToPath(pathIterator.next()))).limit(pathIds.size());
    }

    public int numberOfResults(){
        return pathIds.size();
    }

    private NodeWalkerProc.WalkPath convertIdsToPath(long[] pathIds){
//        NodeWalkerProc.WalkPath path = new NodeWalkerProc.WalkPath(pathIds[1].length);
        int pathLength = pathIds.length;
        NodeWalkerProc.WalkPath path = new NodeWalkerProc.WalkPath(pathLength);
        try (Transaction tx = db.beginTx()) {
            // path[0] contains Ids of nodes, path[1] contains Ids of relationships
            for(int i = 0; i < pathLength - 1; i++){
                Node node = db.getNodeById(pathIds[i]);
                Node nextNode = db.getNodeById(pathIds[i+1]);


                path.addNode(node);
                // TODO find a way to get relationships directly and remove this hack
                Relationship relationship = findRelationshipForNodes(node, nextNode);
                path.addRelationship(relationship);
            }
            // Add last node (that doesn't have a relationship)
            Node node = db.getNodeById(pathIds[pathIds.length - 1]);
            path.addNode(node);
            tx.success();
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
        }
        return path;
    }

    private Relationship findRelationshipForNodes(Node a, Node b){
        for (Relationship relationship:a.getRelationships()){
            if(relationship.getOtherNode(a).equals(b)){
                return relationship;
            }
        }
        return null;
    }
}
