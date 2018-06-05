package org.neo4j.graphalgo.impl.metaPathComputation;

import java.util.HashSet;

public class GetCountThread extends Thread {
    MetaPathComputation parent;
    String threadName;
    HashSet<String> metaPathSet;

    GetCountThread(MetaPathComputation parent, String threadName, HashSet<String> metaPathSet) {
        this.parent = parent;
        this.threadName = threadName;
        this.metaPathSet = metaPathSet;
    }

    public void run() {
        for (String metaPath : metaPathSet) {
            parent.getCount(metaPath);
        }
    }
}
