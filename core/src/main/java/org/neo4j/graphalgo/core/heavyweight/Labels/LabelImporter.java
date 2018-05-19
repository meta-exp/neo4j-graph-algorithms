package org.neo4j.graphalgo.core.heavyweight.Labels;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.Labels.GraphLabeler;
import org.neo4j.graphalgo.core.heavyweight.Labels.LabelMapping;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.*;

public class LabelImporter extends StatementTask<LabelMapping, EntityNotFoundException> {
    private final IdMap mapping;
    private GraphDatabaseAPI api;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.api = api;
        this.mapping = mapping;
    }

    @Override
    public LabelMapping apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();

        LabelMapping labelMapping = new LabelMapping();

        PrimitiveLongIterator relationships =  readOp.relationshipsGetAll();
        while(relationships.hasNext()) {
            long relationshipId = relationships.next();
            readOp.relationshipVisit(relationshipId, (relationship, typeId, startNodeId, endNodeId) ->
                    {
                        labelMapping.putEdgeMapping(startNodeId, endNodeId, typeId);
                        String name = api.getRelationshipById(relationshipId).getType().name();
                        labelMapping.putEdgeStringMapping(typeId, name);
                    });
        }

        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        labelTokens.forEachRemaining(token -> {
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            while (nodesWithThisLabel.hasNext()){
                Integer nodeId = mapping.toMappedNodeId(nodesWithThisLabel.next());
                labelMapping.addNodeMapping(nodeId, token.id());
            }
            labelMapping.putNodeStringMapping(token.id(), token.name());
        });

        return labelMapping;
    }

}
