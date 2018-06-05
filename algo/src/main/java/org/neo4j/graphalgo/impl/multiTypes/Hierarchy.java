package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class Hierarchy extends Algorithm<Hierarchy> {

    private static final String LABEL_PROPERTY = "labels";
    private static final List<String> DEFAULT_LABELS = Arrays.asList("");
    private static final String NAME_PROPERTY = "label";
    public Log log;
    private String typeLabel;
    private GraphDatabaseService db;
    private RelationshipType followLabel;
    private List<Long> currentNodes;


    public Hierarchy(GraphDatabaseService db,
                     String followLabel,
                     Log log) {
        this.log = log;
        this.typeLabel = typeLabel;
        this.db = db;
        this.followLabel = findRelationType(followLabel);
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

    public void compute(long nodeId, int maxDepth) {
        int depth = 0;

        currentNodes.add(nodeId);
        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);
            findNode(nodeInstance, DEFAULT_LABELS, maxDepth, depth);

            transaction.success();
        }

        do {
            final int newDepth = depth;
            depth++;


            currentNodes = currentNodes.parallelStream().map((currentNode) ->
                    processNode(currentNode, maxDepth, newDepth)
            ).collect(() -> new LinkedList<Long>(),
                    (previous, next) -> previous.addAll(next),
                    (left, right) -> left.addAll(right));
        } while (!currentNodes.isEmpty());
    }

    private List<Long> processNode(long nodeId, int maxDepth, int depth) {
        LinkedList<Long> foundNodes = new LinkedList<>();

        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);

            for (Relationship relation : nodeInstance.getRelationships(this.followLabel, Direction.INCOMING)) {
                foundNodes.add(relation.getStartNodeId());
                findNode(relation.getStartNode(),
                        (List<String>) nodeInstance.getProperty(LABEL_PROPERTY),
                        maxDepth,
                        depth);
            }
            transaction.success();
        }

        return foundNodes;
    }

    private void findNode(Node foundNode, List<String> parentLabels, int maxDepth, int depth) {
        if (depth <= maxDepth) {
            parentLabels.add(getLabel(foundNode));
        }
        foundNode.setProperty(LABEL_PROPERTY, parentLabels);
    }


    private String getLabel(Node labelNode) {
        String name = Long.toString(labelNode.getId());
        if (labelNode.hasProperty(NAME_PROPERTY))
            name = (String) labelNode.getProperty(NAME_PROPERTY);

        return name;
    }

    /* Things I don't understand */
    @Override
    public Hierarchy me() {
        return this;
    }

    @Override
    public Hierarchy release() {
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
