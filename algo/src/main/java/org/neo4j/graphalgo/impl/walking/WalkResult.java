package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphdb.Path;

public class WalkResult {
    public Path path;

    public WalkResult(Path path) {
        this.path = path;
    }
}
