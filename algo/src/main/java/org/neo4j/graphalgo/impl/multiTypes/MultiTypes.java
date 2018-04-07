package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiTypes extends Algorithm<MultiTypes> {

    private static final String LABEL_NAME_PROPERTY = "name";
    private HeavyGraph graph;
    private String edgeType;
    private String typeLabel;
    private GraphDatabaseService db;
    private Map<Integer, Label> nodeLabelMap;
    private RelationshipType relationType;

    public MultiTypes(HeavyGraph graph,
                      GraphDatabaseService db,
                      String edgeType,
                      String typeLabel) throws IOException {
        this.graph = graph;
        this.edgeType = edgeType;
        this.typeLabel = typeLabel;
        this.db = db;
        this.nodeLabelMap = new HashMap<>();
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

        graph.forEachNode(this::updateNodeNeighbors);

        return System.currentTimeMillis() - startTime;
    }

    public boolean updateNodeNeighbors(int nodeId) {
        if (!isTypeNode(nodeId))
            return true;

        try (Transaction transaction = db.beginTx()) {
            Node nodeInstance = db.getNodeById((long) nodeId);
            Label label = getOrCreateLabel(nodeId);

            for (Relationship relation : nodeInstance.getRelationships(this.relationType, Direction.INCOMING)) {
                relation.getStartNode().addLabel(label);
            }

            transaction.success();
        }
        return true;
    }

    private boolean isTypeNode(int node) {
        return graph.getLabelIdToNameDict().get(graph.getLabel(node)).equals(typeLabel);
    }

    private Label getOrCreateLabel(int labelNodeId) {
        if (!nodeLabelMap.containsKey(labelNodeId))
            createLabel(labelNodeId);

        return nodeLabelMap.get(labelNodeId);
    }

    private void createLabel(int labelNodeId) {
        Node labelNodeInstance = db.getNodeById(((Number) labelNodeId).longValue());
        String name = Integer.toString(labelNodeId);
        if (labelNodeInstance.hasProperty(LABEL_NAME_PROPERTY))
            name = (String) labelNodeInstance.getProperty(LABEL_NAME_PROPERTY);

        nodeLabelMap.put(labelNodeId, Label.label(name));
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
}
