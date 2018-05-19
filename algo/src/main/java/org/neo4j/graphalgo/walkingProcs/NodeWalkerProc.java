package org.neo4j.graphalgo.walkingProcs;

import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.impl.walking.AbstractWalkOutput;
import org.neo4j.graphalgo.impl.walking.NodeWalker;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.stream.Stream;

public class NodeWalkerProc extends AbstractWalkingProc {

    @Procedure(name = "randomWalk", mode = Mode.READ)
    @Description("Starts a random walk of the specified number of steps at the given start node. " +
            "Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalk(@Name("nodeId") long nodeId,
                                         @Name("steps") long steps,
                                         @Name("walks") long walks,
                                         @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = walker.walkFromNode(output, nodeId, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "randomWalkFromNodeType", mode = Mode.READ)
    @Description("Starts (multiple) random walk(s) of the specified number of steps at multiple random start nodes of the given type. " +
            "If not type is given any node is chosen. Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalkFromNodeType(@Name("steps") long steps,
                                              @Name("walks") long walks,
                                              @Name(value = "type", defaultValue = "") String type,
                                              @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = walker.walkFromNodeType(output, steps, walks, type);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "randomWalkFromAllNodes", mode = Mode.READ)
    @Description("Starts random walks from every node in the graph. Specify the steps for each random walks " +
            "and the number of walks per node. Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> randomWalkFromAllNodes(@Name("steps") long steps,
                                                 @Name("walks") long walks,
                                                 @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getRandomWalker();
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = walker.walkFromAllNodes(output, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "node2vecWalk", mode = Mode.READ)
    @Description("Starts (multiple) node2vecWalk(s) with the given parameters at the given start node. " +
            "Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> node2VecWalk(@Name("nodeId") long nodeId,
                                         @Name("steps") long steps,
                                         @Name("walks") long walks,
                                         @Name("returnParameter") double returnParam,
                                         @Name("inOutParameter") double inOutParam,
                                         @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getNode2VecWalker(returnParam, inOutParam);
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = walker.walkFromNode(output, nodeId, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    @Procedure(name = "node2vecWalkFromAllNodes", mode = Mode.READ)
    @Description("Starts (multiple) node2vecWalk(s) with the given parameters from every node in the graph. " +
            "Optionally specify a filePath instead of returning the paths within neo4j.")
    public Stream<WalkResult> node2VecWalkFromAllNodes(@Name("steps") long steps,
                                         @Name("walks") long walks,
                                         @Name("returnParameter") double returnParam,
                                         @Name("inOutParameter") double inOutParam,
                                         @Name(value = "filePath", defaultValue = "") String filePath) throws IOException {
        NodeWalker walker = getNode2VecWalker(returnParam, inOutParam);
        AbstractWalkOutput output = getAppropriateOutput(filePath);

        Stream<WalkResult> stream = walker.walkFromAllNodes(output, steps, walks);
//        graph.release(); Should have no impact, as done by GC
        return stream;
    }

    private NodeWalker getRandomWalker(){
        HeavyGraph graph = getGraph();
        NodeWalker.AbstractNextNodeStrategy nextNodeStrategy = new NodeWalker.RandomNextNodeStrategy(graph, graph);
        return new NodeWalker(graph, log, nextNodeStrategy);
    }

    private NodeWalker getNode2VecWalker(double returnParam, double inOutParam){
        HeavyGraph graph = getGraph();
        NodeWalker.AbstractNextNodeStrategy nextNodeStrategy = new NodeWalker.Node2VecStrategy(graph, graph, returnParam, inOutParam);
        return new NodeWalker(graph, log, nextNodeStrategy);
    }

}
