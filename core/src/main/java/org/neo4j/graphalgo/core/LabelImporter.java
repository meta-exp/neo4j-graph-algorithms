package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class LabelImporter extends StatementTask<AbstractMap.SimpleEntry<HashMap<Integer, ArrayList<LabelImporter.IdNameTuple>>, HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer>>, EntityNotFoundException> {
    private final IdMap mapping;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.mapping = mapping;
    }

    @Override
    public AbstractMap.SimpleEntry<HashMap<Integer, ArrayList<IdNameTuple>>, HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer>> apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();
        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        PrimitiveLongIterator relationships = readOp.relationshipsGetAll();

        HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> nodesToLabelMap = new HashMap<>();
        while (relationships.hasNext()) {
            long relationshipId = relationships.next();
            readOp.relationshipVisit(relationshipId, (relationship, typeId, startNodeId, endNodeId) -> createEdgeTypeEntry(typeId, startNodeId, endNodeId, nodesToLabelMap));
        }

        HashMap<Integer, ArrayList<IdNameTuple>> idLabelMap = new HashMap<>();
        for (int nodeId = 0; nodeId < readOp.nodesGetCount(); nodeId++) {
            idLabelMap.put(nodeId, new ArrayList<>());
        }
        while (labelTokens.hasNext()) {
            Token token = labelTokens.next();
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            IdNameTuple tuple = new IdNameTuple(token.id(), token.name());
            while (nodesWithThisLabel.hasNext()) {
                Integer nodeId = mapping.toMappedNodeId(nodesWithThisLabel.next());
                idLabelMap.get(nodeId).add(tuple);
            }
        }

        return new AbstractMap.SimpleEntry<>(idLabelMap, nodesToLabelMap);
    }

    private boolean createEdgeTypeEntry(int typeId, long startNodeId, long endNodeId, HashMap<AbstractMap.SimpleEntry<Integer, Integer>, Integer> nodesToLabelMap) {
        AbstractMap.SimpleEntry<Integer, Integer> pair = new AbstractMap.SimpleEntry<>(mapping.toMappedNodeId(startNodeId), mapping.toMappedNodeId(endNodeId));
        nodesToLabelMap.put(pair, typeId);
        return true;
    }

    public class IdNameTuple {
        private int id;
        private String name;

        public IdNameTuple(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }
    }

}
