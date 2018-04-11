package org.neo4j.graphalgo.impl.metaPathComputation;

public class ComputeMetaPathFromNodeLabelThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    int nodeID;
    int metaPathLength;

    ComputeMetaPathFromNodeLabelThread(MetaPathComputation parent, String threadName, int nodeID, int metaPathLength) {
        this.parent = parent;
        this.threadName = threadName;
        this.nodeID = nodeID;
        this.metaPathLength = metaPathLength;
        currentThread().setName(threadName);
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
