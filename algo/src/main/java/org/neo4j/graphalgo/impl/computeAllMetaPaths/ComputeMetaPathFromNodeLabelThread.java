package org.neo4j.graphalgo.impl.computeAllMetaPaths;

public class ComputeMetaPathFromNodeLabelThread extends Thread {
    ComputeAllMetaPaths parent;
    String threadName;
    int nodeLabel;
    int metaPathLength;

    ComputeMetaPathFromNodeLabelThread(ComputeAllMetaPaths parent, String threadName, int nodeLabel, int metaPathLength) {
        this.parent = parent;
        this.threadName = threadName;
        this.nodeLabel = nodeLabel;
        this.metaPathLength = metaPathLength;
    }

    public void run() {
        parent.computeMetaPathFromNodeLabel(nodeLabel, metaPathLength);
    }
}
