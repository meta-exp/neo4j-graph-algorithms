package org.neo4j.graphalgo.impl.metaPathComputation;

public class GetCountThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    String metaPath;

    GetCountThread(MetaPathComputation parent, String threadName, String metaPath) {
        this.parent = parent;
        this.threadName = threadName;
        this.metaPath = metaPath;
    }

    public void run() {
        parent.getCount(metaPath);
    }
}
