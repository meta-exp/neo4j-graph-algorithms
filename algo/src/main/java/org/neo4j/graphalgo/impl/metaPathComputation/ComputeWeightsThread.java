package org.neo4j.graphalgo.impl.metaPathComputation;

import java.util.HashSet;

public class ComputeWeightsThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    HashSet<String> metaPathSet;

    ComputeWeightsThread(MetaPathComputation parent, String threadName, HashSet<String> metaPathSet) {
        this.parent = parent;
        this.threadName = threadName;
        this.metaPathSet = metaPathSet;
    }

    public void run() {
        parent.computeWeights(metaPathSet);
    }
}
