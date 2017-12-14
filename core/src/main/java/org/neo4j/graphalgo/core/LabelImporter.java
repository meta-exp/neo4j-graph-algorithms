package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Token;

import java.util.HashMap;
import java.util.Iterator;

public class LabelImporter extends StatementTask<HashMap<Integer, String>, EntityNotFoundException> {
    private final IdMap mapping;

    public LabelImporter(
            GraphDatabaseAPI api,
            IdMap mapping) {
        super(api);
        this.mapping = mapping;
    }

    @Override
    public HashMap<Integer, String> apply(final Statement statement) throws
            EntityNotFoundException {

        final ReadOperations readOp = statement.readOperations();

        Iterator<Token> labelTokens = readOp.labelsGetAllTokens();
        HashMap<Integer, String> idLabelMap = new HashMap<>();

        while (labelTokens.hasNext()){
            Token token = labelTokens.next();
            PrimitiveLongIterator nodesWithThisLabel = readOp.nodesGetForLabel(token.id());
            while (nodesWithThisLabel.hasNext()){
                idLabelMap.put(mapping.toMappedNodeId(nodesWithThisLabel.next()), token.name());
            }
        }

        return idLabelMap;
    }

}
