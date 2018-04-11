package org.neo4j.graphalgo.impl.metaPathComputation;

public class ComputeMetaPathFromNodeThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    int nodeID;
    int metaPathLength;

    ComputeMetaPathFromNodeThread(MetaPathComputation parent, String threadName, int nodeID, int metaPathLength) {
        this.parent = parent;
        this.threadName = threadName;
        this.nodeID = nodeID;
        this.metaPathLength = metaPathLength;
    }

    public void run() {
        parent.computeMetaPathFromNodeLabel(nodeID, metaPathLength);
    }

    public String getThreadName() {
        return threadName;
    }

    public int getNodeID() {
        return nodeID;
    }
}
