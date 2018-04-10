package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class MultiTypes extends Algorithm<MultiTypes> {

    private static final String LABEL_NAME_PROPERTY = "name";
    public Log log;
    private String typeLabel;
    private GraphDatabaseService db;
    private RelationshipType relationType;

    public MultiTypes(GraphDatabaseService db,
                      String edgeType,
                      String typeLabel,
                      Log log) throws IOException {
        this.log = log;
        this.typeLabel = typeLabel;
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

        LinkedList<Thread> threads = new LinkedList<>();

        for (long nodeId : getTypeNodeIds()) {
            Thread thread = new LabelingThread(this::updateNodeNeighbors, nodeId);
            threads.add(thread);
            thread.run();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage());
            }
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

            Label label = getLabel(nodeInstance);

            for (Relationship relation : nodeInstance.getRelationships(this.relationType, Direction.INCOMING)) {
                relation.getStartNode().addLabel(label);
            }
            transaction.success();
        }

        return true;
    }

    private Label getLabel(Node labelNode) {
        String name = Long.toString(labelNode.getId());
        if (labelNode.hasProperty(LABEL_NAME_PROPERTY))
            name = (String) labelNode.getProperty(LABEL_NAME_PROPERTY);

        return Label.label(name);
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
