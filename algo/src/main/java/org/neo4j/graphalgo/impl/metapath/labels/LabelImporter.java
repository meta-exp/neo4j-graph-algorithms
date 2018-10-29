package org.neo4j.graphalgo.impl.metapath.labels;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

public class LabelImporter extends StatementTask<LabelMapping, EntityNotFoundException> {
    private final IdMapping mapping;

    public LabelImporter(GraphDatabaseAPI api, IdMapping mapping) {
        super(api);
        this.mapping = mapping;
    }

    public static LabelMapping loadMetaData(HeavyGraph graph, GraphDatabaseAPI api) throws EntityNotFoundException {
        try (Transaction tx = api.beginTx()) {
            LabelMapping labelMapping;
            try (Statement statement = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get()) {
                labelMapping = new LabelImporter(api, graph).apply(statement);
            }
            tx.success();
            return labelMapping;
        }
    }

    @Override
    public LabelMapping apply(final Statement statement) throws EntityNotFoundException {
        final ReadOperations readOp = statement.readOperations();

        LabelMapping labelMapping = new LabelMapping(readOp.labelsGetAllTokens(), readOp.relationshipTypesGetAllTokens());
        PrimitiveLongIterator relationships = readOp.relationshipsGetAll();
        while (relationships.hasNext()) {
            long relationshipId = relationships.next();
            readOp.relationshipVisit(relationshipId, (relationship, typeId, startNodeId, endNodeId) ->
            {
                int mapped_start = mapping.toMappedNodeId(startNodeId);
                int mapped_end = mapping.toMappedNodeId(endNodeId);
                labelMapping.putEdgeMapping(mapped_start, mapped_end, (short) typeId);
            });
        }

        PrimitiveLongIterator nodes = readOp.nodesGetAll();
        short[] labelArray = new short[100];
        while (nodes.hasNext()) {
            long id = nodes.next();
            int mappedId = mapping.toMappedNodeId(id);
            PrimitiveIntIterator labels = readOp.nodeGetLabels(id);
            int idx = 0;
            while (labels.hasNext()) {
                if (idx > labelArray.length) {
                    labelArray = Arrays.copyOf(labelArray, labelArray.length + 10);
                }
                labelArray[idx++] = (short)labels.next();
            }
            labelMapping.addNodeMapping(mappedId, Arrays.copyOf(labelArray, idx));
        }
        return labelMapping;
    }

}
