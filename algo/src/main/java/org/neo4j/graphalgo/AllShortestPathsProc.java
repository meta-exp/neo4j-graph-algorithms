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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.AllShortestPaths;
import org.neo4j.graphalgo.impl.HugeMSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.MSBFSASPAlgorithm;
import org.neo4j.graphalgo.impl.MSBFSAllShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class AllShortestPathsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.allShortestPaths.stream")
    @Description("CALL algo.allShortestPaths.stream(weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0, concurrency:4}) " +
            "YIELD sourceNodeId, targetNodeId, distance - yields a stream of {sourceNodeId, targetNodeId, distance}")
    public Stream<AllShortestPaths.Result> allShortestPathsStream(
            @Name(value = "propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withConcurrency(configuration.getConcurrency())
                .withAllocationTracker(tracker)
                .load(configuration.getGraphImpl());

        final MSBFSASPAlgorithm<?> algo;

        // use MSBFS ASP if no weightProperty is set
        if (null == propertyName || propertyName.isEmpty()) {
            if (graph instanceof HugeGraph) {
                HugeGraph hugeGraph = (HugeGraph) graph;
                algo = new HugeMSBFSAllShortestPaths(
                        hugeGraph,
                        tracker,
                        configuration.getConcurrency(),
                        Pools.DEFAULT);
            } else {
                algo = new MSBFSAllShortestPaths(
                        graph,
                        configuration.getConcurrency(),
                        Pools.DEFAULT);
            }
            algo.withProgressLogger(ProgressLogger.wrap(
                    log,
                    "AllShortestPaths(MultiSource)"));
        } else {
            // weighted ASP otherwise
            algo = new AllShortestPaths(graph, Pools.DEFAULT, configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "AllShortestPaths)"));
        }

        return algo.withTerminationFlag(TerminationFlag.wrap(transaction)).resultStream();
    }
}
