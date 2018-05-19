package org.neo4j.graphalgo.walkingProcs;

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.*;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.stream.Stream;

public class MetaPathInstancesProc  extends AbstractWalkingProc{

    @Procedure(name = "metaPathInstances", mode = Mode.READ)
    @Description("Find all instances of the given meta-path. " +
            "Enter the meta-path by concatenating node- and edge-types and separating them with \"%%\"." +
            "Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> metaPathInstances(@Name(value = "metaPath") String metaPath,
                                         @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        MetaPathInstances finder = getMetaPathInstanceFinder();
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = finder.findMetaPathInstances(metaPath, output);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    private MetaPathInstances getMetaPathInstanceFinder(){
        HeavyGraph graph = getGraph();
        return new MetaPathInstances(graph, log);
    }

    protected HeavyGraph getGraph(){
        AllocationTracker tracker = AllocationTracker.create();
        HeavyGraph graph = (HeavyGraph) new GraphLoader(api, Pools.DEFAULT)
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutNodeProperties()
                .withoutExecutorService()
                .withoutRelationshipWeights()
                .withAllocationTracker(tracker)
                .withLabelAsProperty()
                .load(HeavyGraphFactory.class);

        return graph;
    }

}
