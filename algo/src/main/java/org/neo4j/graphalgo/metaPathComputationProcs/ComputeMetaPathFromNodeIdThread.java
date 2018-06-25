package org.neo4j.graphalgo.metaPathComputationProcs;

import com.google.common.collect.Sets;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ComputeMetaPathFromNodeIdThread implements Runnable {
	private final int             start_nodeId;
	private final int             end_nodeID;
	private final int             metaPathLength;
	private final HeavyGraph      graph;
	private final Log             log;
	private final float           edgeSkipProbability;
	private final Random random = new Random(42);

	ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.end_nodeID = end_nodeID;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = 0;
		this.graph = graph;
		this.log = log;
	}

	public ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.end_nodeID = end_nodeID;
		this.metaPathLength = metaPathLength;
		this.edgeSkipProbability = edgeSkipProbability;
		this.graph = graph;
		this.log = log;
	}

	public void computeMetaPathFromNodeID(int start_nodeId, int end_nodeID, int metaPathLength) {
		ArrayList<Integer[]> initialMetaPath = new ArrayList<>();

		ArrayList<ArrayList<Integer[]>> multiTypeMetaPaths = computeMetaPathFromNodeID(initialMetaPath, start_nodeId, end_nodeID, metaPathLength - 1);
		List<String> metaPaths = multiTypeMetaPaths.parallelStream().map(this::returnMetaPaths).collect(ArrayList<String>::new, ArrayList::addAll, ArrayList::addAll);
		log.info("Calculated meta-paths between " + start_nodeId + " and " + end_nodeID + " save in " + new File("/tmp/between_instances").getAbsolutePath());
		if (!new File("/tmp/between_instances").exists()) {
			new File("/tmp/between_instances").mkdir();
		}
		try {
			PrintStream out = new PrintStream(new FileOutputStream(
					"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
							.toOriginalNodeId(end_nodeID) + ".txt"));
			for (String mp : metaPaths) {
				out.println(mp);
			}
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			log.error("FileNotFoundException occured: " + e.toString());
		}
	}

	private ArrayList<ArrayList<Integer[]>> computeMetaPathFromNodeID(ArrayList<Integer[]> currentMultiTypeMetaPath, int currentInstance, int end_nodeID, int metaPathLength) {
		if (metaPathLength <= 0)
			return new ArrayList<ArrayList<Integer[]>>();

		if (currentInstance == end_nodeID) {
			ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<Integer[]>(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(graph.getLabels(currentInstance));
			ArrayList<ArrayList<Integer[]>> baseList = new ArrayList<ArrayList<Integer[]>>();
			baseList.add(newMultiTypeMetaPath);
			return baseList;
		}

		Integer[] labels = graph.getLabels(currentInstance);
		return IntStream.of(graph.getAdjacentNodes(currentInstance)).filter(x -> random.nextFloat() > this.edgeSkipProbability).mapToObj(node -> {
			ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<Integer[]>(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(labels);
			newMultiTypeMetaPath.add(new Integer[] { graph.getEdgeLabel(currentInstance, node) });
			return computeMetaPathFromNodeID(newMultiTypeMetaPath, node, end_nodeID, metaPathLength - 1);
		}).collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
	}

	public List<String> returnMetaPaths(ArrayList<Integer[]> metaPathParts) {
		Set<List<Integer>> allMetaPaths = composeMetaPaths(metaPathParts);
		return stringifyMetaPaths(allMetaPaths);
	}

	public List<String> stringifyMetaPaths(Set<List<Integer>> allMetaPaths) {
		return allMetaPaths.parallelStream().map(list -> list.stream().map(Object::toString).collect(Collectors.joining("|"))).collect(Collectors.toList());
	}

	public Set<List<Integer>> composeMetaPaths(ArrayList<Integer[]> metaPathParts) {
		List<Set<Integer>> interimList = metaPathParts.parallelStream().map(list -> new HashSet<Integer>(Arrays.asList(list))).collect(Collectors.toList());
		return Sets.cartesianProduct(interimList);
	}

	public void run() {
		computeMetaPathFromNodeID(start_nodeId, end_nodeID, metaPathLength);
	}
}
