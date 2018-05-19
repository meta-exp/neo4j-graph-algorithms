package org.neo4j.graphalgo.impl.walking;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class WalkPath implements Path {
    private ArrayList<Node> nodes;
    private ArrayList<Relationship> relationships;

    public WalkPath(int size) {
        nodes = new ArrayList<>(size);
        relationships = new ArrayList<>(Math.max(0, size - 1)); // for empty paths
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void addRelationship(Relationship relationship) {
        relationships.add(relationship);
    }

    @Override
    public Node startNode() {
        return nodes.get(0);
    }

    @Override
    public Node endNode() {
        return nodes.get(nodes.size() - 1);
    }

    @Override
    public Relationship lastRelationship() {
        return relationships.get(relationships.size() - 1);
    }

    @Override
    public Iterable<Relationship> relationships() {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        ArrayList<Relationship> reverse = new ArrayList<>(relationships);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public Iterable<Node> nodes() {
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes() {
        ArrayList<Node> reverse = new ArrayList<>(nodes);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public int length() {
        return nodes.size();
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    @Override
    public Iterator<PropertyContainer> iterator() {
        //TODO ???????
        return null;
    }
}
