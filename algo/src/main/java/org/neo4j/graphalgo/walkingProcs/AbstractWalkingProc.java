package org.neo4j.graphalgo.walkingProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.AbstractWalkOutput;
import org.neo4j.graphalgo.impl.walking.WalkDatabaseOutput;
import org.neo4j.graphalgo.impl.walking.WalkNodeDirectFileOutput;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.io.IOException;

public class AbstractWalkingProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    protected HeavyGraph getGraph(){
        AllocationTracker tracker = AllocationTracker.create();
        HeavyGraph graph = (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutNodeProperties()
                .withoutExecutorService()
                .withoutRelationshipWeights()
                .withAllocationTracker(tracker)
                .load(HeavyGraphFactory.class);

        return graph;
    }

    protected AbstractWalkOutput getAppropriateOutput(String filePath) throws IOException{
        AbstractWalkOutput output;
        if(filePath.isEmpty()){
            output = new WalkDatabaseOutput(db, log);
        } else {
            output = new WalkNodeDirectFileOutput(filePath);
        }
        return output;
    }

}
