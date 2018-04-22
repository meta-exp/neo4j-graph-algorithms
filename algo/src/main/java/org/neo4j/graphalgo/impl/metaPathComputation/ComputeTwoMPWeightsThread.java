package org.neo4j.graphalgo.impl.metaPathComputation;

import java.util.HashSet;

public class ComputeTwoMPWeightsThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    HashSet<Integer> labelIDSet;

    ComputeTwoMPWeightsThread(MetaPathComputation parent, String threadName, HashSet<Integer> labelIDSet) {
        this.parent = parent;
        this.threadName = threadName;
        this.labelIDSet = labelIDSet;
    }

    public void run() {
        parent.computeTwoMPWeights(labelIDSet);
    }
}
