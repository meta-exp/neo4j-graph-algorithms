package org.neo4j.graphalgo.impl.metapath;

import org.neo4j.graphalgo.impl.Algorithm;
import java.util.HashSet;

public class MetaPathComputation extends Algorithm<MetaPathComputation> {
    public void computeMetaPathFromNodeLabel(int startNodeLabel, int pMetaPathLength) {
        //override this
    }

    public void getCount(String metaPath) {
        //override this
    }

    public void computeWeights(HashSet<String> metaPathSet) {
        //override this
    }

    public void computeTwoMPWeights(HashSet<Integer> labelIDSet) {
        //override this
    }


    @Override
    public MetaPathComputation me() { return this; }

    @Override
    public MetaPathComputation release() {
        return null;
    }
}
