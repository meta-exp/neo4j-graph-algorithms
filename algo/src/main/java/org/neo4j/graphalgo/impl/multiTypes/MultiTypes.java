package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MultiTypes extends Algorithm<MultiTypes> {

    private String labelNameProperty = "name";
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
    public Log log;
    private String typeLabel;
    private GraphDatabaseService db;
    private RelationshipType relationType;

    public MultiTypes(GraphDatabaseService db,
                      String edgeType,
                      String typeLabel,
                      String labelNameProperty,
                      Log log) {
        this.log = log;
        this.typeLabel = typeLabel;
        this.labelNameProperty = labelNameProperty;
        this.db = db;
        this.relationType = findRelationType(edgeType);
    }

    private RelationshipType findRelationType(String edgeType) {
        RelationshipType returnType = null;
        try (Transaction transaction = db.beginTx()) {
            for (RelationshipType type : db.getAllRelationshipTypes()) {
                if (type.name().equals(edgeType)) {
                    returnType = type;
                    break;
                }
            }
            transaction.success();
        }
        return returnType;
    }

    public void compute() {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        for (long nodeId : getTypeNodeIds()) {
            Thread thread = new LabelingThread(this::updateNodeNeighbors, nodeId);
            executor.execute(thread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Thread join timed out");
        }
    }

    private List<Long> getTypeNodeIds() {
        LinkedList<Long> typeNodeIds = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            ResourceIterator<Node> allNodes = db.findNodes(Label.label(typeLabel));
            while (allNodes.hasNext()) {
                Node node = allNodes.next();
                typeNodeIds.add(node.getId());
            }
            allNodes.close();

            transaction.success();
        }

        return typeNodeIds;
    }


    public boolean updateNodeNeighbors(long nodeId) {
        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);

            Label[] labels = getLabels(nodeInstance);

            for (Relationship relation : nodeInstance.getRelationships(this.relationType, Direction.INCOMING)) {
                for(Label label: labels) {
                    relation.getStartNode().addLabel(label);
                }
            }
            transaction.success();
        }

        return true;
    }

    private Label[] getLabels(Node labelNode) {
        ArrayList<Label> labels = new ArrayList<>();
        for(Label label: labelNode.getLabels()) {
            labels.add(label);
        }
        return (Label[]) labels.toArray();
    }

    /* Things I don't understand */
    @Override
    public MultiTypes me() {
        return this;
    }

    @Override
    public MultiTypes release() {
        return null;
    }

    class LabelingThread extends Thread {
        private Consumer<Long> updateMethod;
        private long typeNode;

        LabelingThread(Consumer<Long> updateMethod, long typeNode) {
            this.updateMethod = updateMethod;
            this.typeNode = typeNode;
        }

        public void run() {
            this.updateMethod.accept(typeNode);
        }
    }

}
