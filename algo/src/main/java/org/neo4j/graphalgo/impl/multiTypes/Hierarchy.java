package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.stream.StreamSupport;

public class Hierarchy extends Algorithm<Hierarchy> {

    public Log log;
    private String nameProperty;
    private String typeLabelName;
    private Label typeLabel;
    private GraphDatabaseService db;
    private RelationshipType followLabel;
    private Set<Long> currentNodes = new HashSet<>();
    private int count = 0;


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
        findNode(nodeId, new ArrayList<>(), maxDepth, depth);

        do {
            depth++;
            log.info("Starting loop: " + depth);
            final int newDepth = depth;

            currentNodes = currentNodes.stream().map((currentNode) ->
                    processNode(currentNode, maxDepth, newDepth)
            ).collect(() -> new HashSet<>(),
                    (previous, next) -> previous.addAll(next),
                    (left, right) -> left.addAll(right));
            log.info("Size of current nodes is " + currentNodes.size()+ " in depth " + depth);
            log.info(Integer.toString(count));
            System.out.println(count);
            count = 0;
        } while (!currentNodes.isEmpty());
    }

    public List<Long> processNode(long nodeId, int maxDepth, int depth) {
        count = count + 1;
        //log.info("Process node " + nodeId);
        LinkedList<Long> foundNodes = new LinkedList<>();
        Iterable<Label> labels;

        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById(nodeId);
            labels = nodeInstance.getLabels();

            StreamSupport
                    .stream(nodeInstance.getRelationships(this.followLabel, Direction.INCOMING).spliterator(), false)
                    .forEach((relation) -> {
                        foundNodes.add(relation.getStartNodeId());
                    });
            transaction.success();
        }

        foundNodes.stream().forEach(foundNode ->
                findNode(foundNode,
                labels,
                maxDepth,
                depth));
        //log.info("Found "+foundNodes.size()+ " nodes while processing node " + nodeId);
        return foundNodes;
    }

    private void findNode(long foundNodeId, Iterable<Label> parentLabels, int maxDepth, int depth) {

        try (Transaction transaction = db.beginTx()) {
            Node foundNode = db.getNodeById(foundNodeId);
            for (Label label : parentLabels) {
                foundNode.addLabel(label);
            }

            Label ownLabel = getLabel(foundNode);
            if (depth <= maxDepth && ownLabel != null) {
                foundNode.addLabel(ownLabel);
            }

            if (!typeLabelName.isEmpty()) {
                foundNode.addLabel(typeLabel);
            }

            transaction.success();
        }
    }


    private Label getLabel(Node labelNode) {
        if (labelNode.hasProperty(nameProperty) && !labelNode.getProperty(nameProperty).equals(""))
            return Label.label((String) labelNode.getProperty(nameProperty));
        return null;
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
