package org.neo4j.graphalgo.impl.metaPathComputation.getSchema;

public class Pair {
    int car = 0;
    int cdr = 0;

    public int car()
    {
        return car;
    }

    public int cdr()
    {
        return cdr;
    }

    public void setCar(int value)
    {
        car = value;
    }

    public void setCdr(int value)
    {
        cdr = value;
    }
}