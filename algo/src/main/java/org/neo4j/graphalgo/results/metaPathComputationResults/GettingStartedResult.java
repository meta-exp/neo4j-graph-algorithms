package org.neo4j.graphalgo.results.metaPathComputationResults;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

/**
 * @author mknblch
 */
public class GettingStartedResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final boolean hasEdge;

    private GettingStartedResult(Long loadMillis, Long computeMillis, Long writeMillis, boolean hasEdge) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.hasEdge = hasEdge;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<GettingStartedResult> {

        private boolean hasEdge = false;

        public GettingStartedResult build() {
            return new GettingStartedResult(loadDuration, evalDuration, writeDuration, hasEdge);
        }
    }
}
