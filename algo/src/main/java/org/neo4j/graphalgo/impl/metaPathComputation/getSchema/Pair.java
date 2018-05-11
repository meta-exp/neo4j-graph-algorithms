package org.neo4j.graphalgo.impl.metaPathComputation.getSchema;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public final class Pair {
    private final int car;
    private final int cdr;

    public Pair(int car, int cdr) {
        this.car = car;
        this.cdr = cdr;
    }

    public int car() {
        return car;
    }

    public int cdr() {
        return cdr;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (obj == this)
            return true;

        Pair pair = (Pair) obj;
        return new EqualsBuilder().
                append(car, pair.car).
                append(cdr, pair.cdr).
                isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
                append(car).
                append(cdr).
                toHashCode();
    }
}