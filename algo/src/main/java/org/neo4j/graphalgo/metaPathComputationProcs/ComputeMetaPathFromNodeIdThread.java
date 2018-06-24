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

public class ComputeMetaPathFromNodeIdThread implements Runnable {
	private final int             start_nodeId;
	private final int             end_nodeID;
	private final int             metaPathLength;
	private final HeavyGraph      graph;
	private final Log             log;
	private final float           edgeSkipProbability;
	private final Random random = new Random(42);
	private       HashSet<String> duplicateFreeMetaPathsOfThread;

	ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.end_nodeID = end_nodeID;
		this.metaPathLength = metaPathLength;
		this.duplicateFreeMetaPathsOfThread = new HashSet<>();
		this.edgeSkipProbability = 0;
		this.graph = graph;
		this.log = log;
	}

	public ComputeMetaPathFromNodeIdThread(int start_nodeId, int end_nodeID, int metaPathLength, float edgeSkipProbability, HeavyGraph graph, Log log) {
		this.start_nodeId = start_nodeId;
		this.end_nodeID = end_nodeID;
		this.metaPathLength = metaPathLength;
		this.duplicateFreeMetaPathsOfThread = new HashSet<>();
		this.edgeSkipProbability = edgeSkipProbability;
		this.graph = graph;
		this.log = log;
	}

	public void computeMetaPathFromNodeID(int start_nodeId, int end_nodeID, int metaPathLength) {
		ArrayList<Integer[]> initialMetaPath = new ArrayList<>();

		computeMetaPathFromNodeID(initialMetaPath, start_nodeId, end_nodeID, metaPathLength - 1);
		log.info("Calculated meta-paths between " + start_nodeId + " and " + end_nodeID + " save in " + new File("/tmp/between_instances").getAbsolutePath());
		if (!new File("/tmp/between_instances").exists()) {
			new File("/tmp/between_instances").mkdir();
		}
		try {
			PrintStream out = new PrintStream(new FileOutputStream(
					"/tmp/between_instances/MetaPaths-" + metaPathLength + "-" + this.edgeSkipProbability + "_" + graph.toOriginalNodeId(start_nodeId) + "_" + graph
							.toOriginalNodeId(end_nodeID) + ".txt"));
			for (String mp : duplicateFreeMetaPathsOfThread) {
				out.println(mp);
			}
			out.flush();
			out.close();
			duplicateFreeMetaPathsOfThread = null;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			log.error("FileNotFoundException occured: " + e.toString());
		}
	}

	private void computeMetaPathFromNodeID(ArrayList<Integer[]> currentMultiTypeMetaPath, int currentInstance, int end_nodeID, int metaPathLength) {
		if (metaPathLength <= 0)
			return;

		if (currentInstance == end_nodeID) {
			ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<Integer[]>(currentMultiTypeMetaPath);
			newMultiTypeMetaPath.add(graph.getLabels(currentInstance));
			List<String> metaPaths = returnMetaPaths(newMultiTypeMetaPath);
			duplicateFreeMetaPathsOfThread.addAll(metaPaths);
			return;
		}

		Integer[] labels = graph.getLabels(currentInstance);
		for (int node : graph.getAdjacentNodes(currentInstance)) {
			if (random.nextFloat() > this.edgeSkipProbability) {
				ArrayList<Integer[]> newMultiTypeMetaPath = new ArrayList<Integer[]>(currentMultiTypeMetaPath);
				newMultiTypeMetaPath.add(labels);
				newMultiTypeMetaPath.add(new Integer[] { graph.getEdgeLabel(currentInstance, node) });
				computeMetaPathFromNodeID(newMultiTypeMetaPath, node, end_nodeID, metaPathLength - 1);
			}
		}
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
