package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.Test;
import org.neo4j.graphalgo.impl.metaPathComputation.ComputeMetaPathFromNodeIdThread;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class ComputeMetaPathFromNodeIdThreadTest {

	@Test public void testComputeMetaPaths() throws Exception {
		ComputeMetaPathFromNodeIdThread thread = new ComputeMetaPathFromNodeIdThread(0, 0, 0, 0, null, null);

		ArrayList<Integer[]> metapath_parts = new ArrayList<>();
		metapath_parts.add(new Integer[] { 0 });
		metapath_parts.add(new Integer[] { 1, 2 });
		metapath_parts.add(new Integer[] { 3 });
		metapath_parts.add(new Integer[] { 4, 5 });

		Set<List<Integer>> result = thread.composeMetaPaths(metapath_parts);
		System.out.println(result);

		ArrayList<List<Integer>> expected_result = new ArrayList<>();
		expected_result.add(Arrays.asList(0, 1, 3, 4));
		expected_result.add(Arrays.asList(0, 1, 3, 5));
		expected_result.add(Arrays.asList(0, 2, 3, 4));
		expected_result.add(Arrays.asList(0, 2, 3, 5));

		assertThat(expected_result, containsInAnyOrder(result.toArray()));
	}

	@Test public void testStringifyMetaPaths() {
		ComputeMetaPathFromNodeIdThread thread = new ComputeMetaPathFromNodeIdThread(0, 0, 0, 0, null, null);

		ArrayList<Integer[]> metapath_parts = new ArrayList<>();
		metapath_parts.add(new Integer[] { 0 });
		metapath_parts.add(new Integer[] { 1, 2 });
		metapath_parts.add(new Integer[] { 3 });

		List<String> strings = thread.returnMetaPaths(metapath_parts);
		List<String> expected = new ArrayList<String>();
		expected.add("0|1|3");
		expected.add("0|2|3");
		assertThat(expected, containsInAnyOrder(strings.toArray()));
	}
}