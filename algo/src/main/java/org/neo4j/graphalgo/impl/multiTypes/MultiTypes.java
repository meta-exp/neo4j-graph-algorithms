package org.neo4j.graphalgo.impl.multiTypes;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiTypes extends Algorithm<MultiTypes> {

    private HeavyGraph graph;
    private String edgeType;
    private String typeLabel;
    private GraphDatabaseService db;
    private Map<Integer, Label> nodeLabelMap;

    private static final String LABEL_NAME_PROPERTY = "name";

    public MultiTypes(HeavyGraph graph,
                      GraphDatabaseService db,
                      String edgeType,
                      String typeLabel) throws IOException {
        this.graph = graph;
        this.edgeType = edgeType;
        this.typeLabel = typeLabel;
        this.db = db;
        this.nodeLabelMap = new HashMap<>();

    }

    public long compute() {
        long startTime = System.currentTimeMillis();

        try(Transaction transaction = db.beginTx()) {
            graph.forEachNode(this::updateNode);
            transaction.success();
        }

        return System.currentTimeMillis() - startTime;
    }

    private boolean updateNode(int nodeId) {
//        TODO: Should type nodes also be labeled?
//        if (isTypeNode(nodeId))
//            return true;

        Node nodeInstance = db.getNodeById((long) nodeId);

        for (int neighborId : graph.getOutgoingNodes(nodeId)) {
            if (isTypeNode(neighborId)) {
                nodeInstance.addLabel(getOrCreateLabel(neighborId));
            }
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
        Node labelNodeInstance = db.getNodeById(((Number)labelNodeId).longValue());
        String name = Integer.toString(labelNodeId);
        if (labelNodeInstance.hasProperty(LABEL_NAME_PROPERTY))
            name = (String) labelNodeInstance.getProperty(LABEL_NAME_PROPERTY);

        nodeLabelMap.put(labelNodeId, Label.label(name));
    }



    /* Things I don't understand */
    @Override
    public MultiTypes me() { return this; }

    @Override
    public MultiTypes release() {
        return null;
    }
}
