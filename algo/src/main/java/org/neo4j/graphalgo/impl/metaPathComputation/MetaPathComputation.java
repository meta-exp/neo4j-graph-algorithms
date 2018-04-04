package org.neo4j.graphalgo.impl.metaPathComputation;

import org.neo4j.graphalgo.impl.Algorithm;
import java.util.HashSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MetaPathComputation extends Algorithm<MetaPathComputation> {
    public void computeMetaPathFromNodeLabel(int startNodeLabel, int pMetaPathLength) {
        //override this
    }

    public Stream<ComputeAllMetaPaths.Result> resultStream() {
        return IntStream.range(0, 1).mapToObj(result -> new ComputeAllMetaPaths.Result(new HashSet<>()));
    }

    @Override
    public MetaPathComputation me() { return this; }

    @Override
    public MetaPathComputation release() {
        return null;
    }
}
