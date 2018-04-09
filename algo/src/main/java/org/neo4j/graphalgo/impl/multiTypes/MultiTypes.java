package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.util.function.Consumer;

public class MultiTypes extends Algorithm<MultiTypes> {

    private static final String LABEL_NAME_PROPERTY = "name";
    private String typeLabel;
    private GraphDatabaseService db;
    private RelationshipType relationType;

    public MultiTypes(GraphDatabaseService db,
                      String edgeType,
                      String typeLabel) throws IOException {
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

    public long compute() {
        long startTime = System.currentTimeMillis();

        try (Transaction transaction = db.beginTx()) {

            ResourceIterator<Node> allNodes = db.findNodes(Label.label(typeLabel));
            while (allNodes.hasNext()) {
                Node node = allNodes.next();
                new LabelingThread(this::updateNodeNeighbors, node).run();
//                this.updateNodeNeighbors(node);
            }
            allNodes.close();

            transaction.success();
        }
        return System.currentTimeMillis() - startTime;
    }

    public boolean updateNodeNeighbors(Node nodeInstance) {

        Label label = getLabel(nodeInstance);

        for (Relationship relation : nodeInstance.getRelationships(this.relationType, Direction.INCOMING)) {
            relation.getStartNode().addLabel(label);
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
        private Consumer<Node> updateMethod;
        private Node typeNode;

        LabelingThread(Consumer<Node> updateMethod, Node typeNode) {
            this.updateMethod = updateMethod;
            this.typeNode = typeNode;
        }

        public void run() {
            this.updateMethod.accept(typeNode);
        }
    }

}
