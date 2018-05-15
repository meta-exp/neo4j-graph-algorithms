package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.NodeWalkerProc;

import java.util.stream.Stream;

public abstract class AbstractWalkOutput {
    public AbstractWalkOutput(){
    }

    public abstract void endInput();

    public abstract void addResult(long[][] result);

    public Stream<NodeWalkerProc.WalkResult> getStream() {
        return Stream.empty();
    }

    public abstract int numberOfResults();
}
