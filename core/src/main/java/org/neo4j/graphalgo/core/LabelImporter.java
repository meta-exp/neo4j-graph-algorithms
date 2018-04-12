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

public class LabelImporter extends StatementTask<AbstractMap.SimpleEntry<HashMap<Integer, ArrayList<Object>>, HashMap<AbstractMap.SimpleEntry<Long, Long>, Integer>>, EntityNotFoundException> {
    private final IdMap mapping;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.mapping = mapping;
    }

    @Override
    public AbstractMap.SimpleEntry<HashMap<Integer, ArrayList<Object>>, HashMap<AbstractMap.SimpleEntry<Long, Long>, Integer>> apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();
        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        PrimitiveLongIterator relationships =  readOp.relationshipsGetAll();

        HashMap<AbstractMap.SimpleEntry<Long, Long>, Integer> nodesToLabelMap = new HashMap<>();
        while(relationships.hasNext()) {
            long relationshipId = relationships.next();
            readOp.relationshipVisit(relationshipId, (relationship, typeId, startNodeId, endNodeId) -> createEdgeTypeEntry(typeId, startNodeId, endNodeId, nodesToLabelMap));
        }

        HashMap<Integer, ArrayList<Object>> idLabelMap = new HashMap<>();
        while (labelTokens.hasNext()) {
            Token token = labelTokens.next();
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            while (nodesWithThisLabel.hasNext()){
                ArrayList<Object> idAndName = new ArrayList<>();
                idAndName.add(token.id());
                idAndName.add(token.name());
                idLabelMap.put(mapping.toMappedNodeId(nodesWithThisLabel.next()), idAndName);
            }
        }

        return new AbstractMap.SimpleEntry<>(idLabelMap, nodesToLabelMap);
    }

    private boolean createEdgeTypeEntry(int typeId, long startNodeId, long endNodeId, HashMap<AbstractMap.SimpleEntry<Long, Long>, Integer> nodesToLabelMap)
    {
        AbstractMap.SimpleEntry<Long, Long> pair = new AbstractMap.SimpleEntry<>(startNodeId, endNodeId);
        nodesToLabelMap.put(pair, typeId);
        return true;
    }

}
