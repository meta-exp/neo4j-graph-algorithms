/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * @author mknblch
 */
public class MSClosenessCentrality extends MSBFSCCAlgorithm<MSClosenessCentrality> {

    private Graph graph;
    private AtomicIntegerArray farness;

    private final int concurrency;
    private final ExecutorService executorService;
    private final int nodeCount;

    public MSClosenessCentrality(Graph graph, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        farness = new AtomicIntegerArray(nodeCount);
    }

    @Override
    public MSClosenessCentrality compute() {

        final ProgressLogger progressLogger = getProgressLogger();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.addAndGet(nodeId, len * depth);
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(graph, graph, Direction.OUTGOING, consumer)
                .run(concurrency, executorService);

        return this;
    }

    @Override
    public Stream<Result> resultStream() {
        final double k = nodeCount - 1;
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        centrality(farness.get(nodeId), k)));
    }

    @Override
    public LongToIntFunction farness() {
        return (i) -> farness.get((int) i);
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        final double k = nodeCount - 1;
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<AtomicIntegerArray>)
                        (data, nodeId) -> centrality(data.get((int) nodeId), k));
    }

    @Override
    public MSClosenessCentrality me() {
        return this;
    }

    @Override
    public MSClosenessCentrality release() {
        graph = null;
        farness = null;
        return this;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;

        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
