package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Hierarchy extends Algorithm<Hierarchy> {

    private String nameProperty;
    public Log log;
    private String typeLabelName;
    private Label typeLabel;
    private GraphDatabaseService db;
    private RelationshipType followLabel;
    private Set<Long> currentNodes = new HashSet<>();


    public Hierarchy(GraphDatabaseService db,
                     String followLabel,
                     String nameProperty,
                     String typeLabel,
                     Log log) {
        this.log = log;
        this.typeLabelName = typeLabel;
        this.typeLabel = Label.label(typeLabelName);
        this.db = db;
        this.followLabel = findRelationType(followLabel);
        this.nameProperty = nameProperty;
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
            findNode(nodeInstance, nodeInstance.getLabels(), maxDepth, depth);

            transaction.success();
        }

        do {
            depth++;
            final int newDepth = depth;

            currentNodes = currentNodes.parallelStream().map((currentNode) ->
                    processNode(currentNode, maxDepth, newDepth)
            ).collect(() -> new HashSet<>(),
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
                        nodeInstance.getLabels(),
                        maxDepth,
                        depth);
            }
            transaction.success();
        }

        return foundNodes;
    }

    private void findNode(Node foundNode, Iterable<Label> parentLabels, int maxDepth, int depth) {
        for (Label label : parentLabels) {
            foundNode.addLabel(label);
        }

        if (depth <= maxDepth) {
            foundNode.addLabel(getLabel(foundNode));
        }

        if (!typeLabelName.isEmpty()) {
            foundNode.addLabel(typeLabel);
        }
    }


    private Label getLabel(Node labelNode) {
        String name = Long.toString(labelNode.getId());
        if (labelNode.hasProperty(nameProperty))
            name = (String) labelNode.getProperty(nameProperty);

        return Label.label(name);
    }

    /* Unnecessary abstract methods... */
    @Override
    public Hierarchy me() {
        return this;
    }

    @Override
    public Hierarchy release() {
        return null;
    }

}
