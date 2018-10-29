package org.neo4j.graphalgo.impl.metapath.labels;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.storageengine.api.Token;

import java.util.Iterator;
import java.util.List;

/**
 * @author mh
 * @since 26.06.18
 */
public class Tokens {
    public final short[] ids;
    public final String[] names;

    public Tokens(Iterator<Token> tokens) {
        List<Token> list = Iterators.asList(tokens);
        ids = new short[list.size()];
        names = new String[list.size()];
        int idx = 0;
        for (Token token : list) {
            ids[idx] = (short) token.id();
            names[idx] = token.name();
            idx++;
        }
    }

    public int size() {
        return ids.length;
    }

    public String name(int id) {
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == id) return names[i];
        }
        return null;
    }
}
