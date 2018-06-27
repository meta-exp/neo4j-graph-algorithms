package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.impl.metapath.ComputeAllMetaPaths;
import org.neo4j.graphalgo.impl.metapath.labels.LabelMapping;
import java.util.List;

public class ComputeAllMetaPathsResult {

    public final String metaPath;
    public final long length;
    public final List<Long> pathIds;
    public final long count;

    public ComputeAllMetaPathsResult(ComputeAllMetaPaths.MetaPath metaPath, long count, LabelMapping mapping) {
        this.metaPath = metaPath.toString();
        // this.metaPathText = metaPath.toString(mapping);
        this.length = metaPath.length;
        this.pathIds = metaPath.toIdList();
        this.count = count;
    }
}