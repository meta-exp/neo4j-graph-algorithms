package org.neo4j.graphalgo.impl.metaPathComputation;

public class ComputeMetaPathFromNodeLabelThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    int nodeLabel;
    int metaPathLength;

    ComputeMetaPathFromNodeLabelThread(MetaPathComputation parent, String threadName, int nodeLabel, int metaPathLength) {
        this.parent = parent;
        this.threadName = threadName;
        this.nodeLabel = nodeLabel;
        this.metaPathLength = metaPathLength;
    }

    public void run() {
        parent.computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
    }

    public String getThreadName() {
        return threadName;
    }
}