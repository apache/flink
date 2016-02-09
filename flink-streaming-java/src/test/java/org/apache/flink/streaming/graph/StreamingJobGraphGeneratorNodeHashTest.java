/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.util.NoOpSink;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link StreamNode} hash assignment during translation from {@link StreamGraph} to
 * {@link JobGraph} instances.
 */
public class StreamingJobGraphGeneratorNodeHashTest {

	// ------------------------------------------------------------------------
	// Deterministic hash assignment
	// ------------------------------------------------------------------------

	/**
	 * Creates the same flow twice and checks that all IDs are the same.
	 *
	 * <pre>
	 * [ (src) -> (map) -> (filter) -> (reduce) -> (map) -> (sink) ]
	 *                                                       //
	 * [ (src) -> (filter) ] -------------------------------//
	 *                                                      /
	 * [ (src) -> (filter) ] ------------------------------/
	 * </pre>
	 */
	@Test
	public void testNodeHashIsDeterministic() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		DataStream<String> src0 = env
				.addSource(new NoOpSourceFunction(), "src0")
				.map(new NoOpMapFunction())
				.filter(new NoOpFilterFunction())
				.keyBy(new NoOpKeySelector())
				.reduce(new NoOpReduceFunction()).name("reduce");

		DataStream<String> src1 = env
				.addSource(new NoOpSourceFunction(), "src1")
				.filter(new NoOpFilterFunction());

		DataStream<String> src2 = env
				.addSource(new NoOpSourceFunction(), "src2")
				.filter(new NoOpFilterFunction());

		src0.map(new NoOpMapFunction())
				.union(src1, src2)
				.addSink(new NoOpSinkFunction()).name("sink");

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		final Map<JobVertexID, String> ids = rememberIds(jobGraph);

		// Do it again and verify
		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		src0 = env
				.addSource(new NoOpSourceFunction(), "src0")
				.map(new NoOpMapFunction())
				.filter(new NoOpFilterFunction())
				.keyBy(new NoOpKeySelector())
				.reduce(new NoOpReduceFunction()).name("reduce");

		src1 = env
				.addSource(new NoOpSourceFunction(), "src1")
				.filter(new NoOpFilterFunction());

		src2 = env
				.addSource(new NoOpSourceFunction(), "src2")
				.filter(new NoOpFilterFunction());

		src0.map(new NoOpMapFunction())
				.union(src1, src2)
				.addSink(new NoOpSinkFunction()).name("sink");

		jobGraph = env.getStreamGraph().getJobGraph();

		verifyIdsEqual(jobGraph, ids);
	}

	/**
	 * Verifies that parallelism affects the node hash.
	 */
	@Test
	public void testNodeHashParallelism() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.disableOperatorChaining();

		env.addSource(new NoOpSourceFunction(), "src").setParallelism(4)
				.addSink(new NoOpSink<String>()).name("sink").setParallelism(4);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		Map<JobVertexID, String> ids = rememberIds(jobGraph);

		// Change parallelism of source
		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.disableOperatorChaining();

		env.addSource(new NoOpSourceFunction(), "src").setParallelism(8)
				.addSink(new NoOpSink<String>()).name("sink").setParallelism(4);

		jobGraph = env.getStreamGraph().getJobGraph();

		verifyIdsNotEqual(jobGraph, ids);

		// Change parallelism of sink
		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.disableOperatorChaining();

		env.addSource(new NoOpSourceFunction(), "src").setParallelism(4)
				.addSink(new NoOpSink<String>()).name("sink").setParallelism(8);

		jobGraph = env.getStreamGraph().getJobGraph();

		// The source hash will should be the same
		JobVertex[] vertices = jobGraph.getVerticesAsArray();
		if (vertices[0].isInputVertex()) {
			assertTrue(ids.containsKey(vertices[0].getID()));
			assertFalse(ids.containsKey(vertices[1].getID()));
		}
		else {
			assertTrue(ids.containsKey(vertices[1].getID()));
			assertFalse(ids.containsKey(vertices[0].getID()));
		}
	}

	/**
	 * Tests that there are no collisions with two identical sources.
	 *
	 * <pre>
	 * [ (src0) ] --\
	 *               +--> [ (sink) ]
	 * [ (src1) ] --/
	 * </pre>
	 */
	@Test
	public void testNodeHashIdenticalSources() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.disableOperatorChaining();

		DataStream<String> src0 = env.addSource(new NoOpSourceFunction());
		DataStream<String> src1 = env.addSource(new NoOpSourceFunction());

		src0.union(src1).addSink(new NoOpSinkFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertTrue(vertices.get(0).isInputVertex());
		assertTrue(vertices.get(1).isInputVertex());

		assertNotNull(vertices.get(0).getID());
		assertNotNull(vertices.get(1).getID());

		assertNotEquals(vertices.get(0).getID(), vertices.get(1).getID());
	}

	/**
	 * Tests that (un)chaining affects the node hash (for sources).
	 *
	 * <pre>
	 * A (chained): [ (src0) -> (map) -> (filter) -> (sink) ]
	 * B (unchained): [ (src0) ] -> [ (map) -> (filter) -> (sink) ]
	 * </pre>
	 *
	 * The hashes for the single vertex in A and the source vertex in B need to be different.
	 */
	@Test
	public void testNodeHashAfterSourceUnchaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction())
				.map(new NoOpMapFunction())
				.filter(new NoOpFilterFunction())
				.addSink(new NoOpSinkFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		JobVertexID sourceId = jobGraph.getVerticesSortedTopologicallyFromSources()
				.get(0).getID();

		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction())
				.map(new NoOpMapFunction())
				.startNewChain()
				.filter(new NoOpFilterFunction())
				.addSink(new NoOpSinkFunction());

		jobGraph = env.getStreamGraph().getJobGraph();

		JobVertexID unchainedSourceId = jobGraph.getVerticesSortedTopologicallyFromSources()
				.get(0).getID();

		assertNotEquals(sourceId, unchainedSourceId);
	}

	/**
	 * Tests that (un)chaining affects the node hash (for intermediate nodes).
	 *
	 * <pre>
	 * A (chained): [ (src0) -> (map) -> (filter) -> (sink) ]
	 * B (unchained): [ (src0) ] -> [ (map) -> (filter) -> (sink) ]
	 * </pre>
	 *
	 * The hashes for the single vertex in A and the source vertex in B need to be different.
	 */
	@Test
	public void testNodeHashAfterIntermediateUnchaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction())
				.map(new NoOpMapFunction()).name("map")
				.startNewChain()
				.filter(new NoOpFilterFunction())
				.addSink(new NoOpSinkFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		JobVertex chainedMap = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		assertTrue(chainedMap.getName().startsWith("map"));
		JobVertexID chainedMapId = chainedMap.getID();

		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction())
				.map(new NoOpMapFunction()).name("map")
				.startNewChain()
				.filter(new NoOpFilterFunction())
				.startNewChain()
				.addSink(new NoOpSinkFunction());

		jobGraph = env.getStreamGraph().getJobGraph();

		JobVertex unchainedMap = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		assertEquals("map", unchainedMap.getName());
		JobVertexID unchainedMapId = unchainedMap.getID();

		assertNotEquals(chainedMapId, unchainedMapId);
	}

	/**
	 * Tests that there are no collisions with two identical intermediate nodes connected to the
	 * same predecessor.
	 *
	 * <pre>
	 *             /-> [ (map) ] -> [ (sink) ]
	 * [ (src) ] -+
	 *             \-> [ (map) ] -> [ (sink) ]
	 * </pre>
	 */
	@Test
	public void testNodeHashIdenticalNodes() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.disableOperatorChaining();

		DataStream<String> src = env.addSource(new NoOpSourceFunction());

		src.map(new NoOpMapFunction()).addSink(new NoOpSinkFunction());

		src.map(new NoOpMapFunction()).addSink(new NoOpSinkFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		Set<JobVertexID> vertexIds = new HashSet<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			assertTrue(vertexIds.add(vertex.getID()));
		}
	}

	// ------------------------------------------------------------------------
	// Manual hash assignment
	// ------------------------------------------------------------------------

	/**
	 * Tests that manual hash assignments are mapped to the same operator ID.
	 *
	 * <pre>
	 *                     /-> [ (map) ] -> [ (sink)@sink0 ]
	 * [ (src@source ) ] -+
	 *                     \-> [ (map) ] -> [ (sink)@sink1 ]
	 * </pre>
	 *
	 * <pre>
	 *                    /-> [ (map) ] -> [ (reduce) ] -> [ (sink)@sink0 ]
	 * [ (src)@source ] -+
	 *                   \-> [ (map) ] -> [ (reduce) ] -> [ (sink)@sink1 ]
	 * </pre>
	 */
	@Test
	public void testManualHashAssignment() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.disableOperatorChaining();

		DataStream<String> src = env.addSource(new NoOpSourceFunction())
				.name("source").uid("source");

		src.map(new NoOpMapFunction())
				.addSink(new NoOpSinkFunction())
				.name("sink0").uid("sink0");

		src.map(new NoOpMapFunction())
				.addSink(new NoOpSinkFunction())
				.name("sink1").uid("sink1");

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		Set<JobVertexID> ids = new HashSet<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			assertTrue(ids.add(vertex.getID()));
		}

		// Resubmit a slightly different program
		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.disableOperatorChaining();

		src = env.addSource(new NoOpSourceFunction())
				// New map function, should be mapped to the source state
				.map(new NoOpMapFunction())
				.name("source").uid("source");

		src.map(new NoOpMapFunction())
				.keyBy(new NoOpKeySelector())
				.reduce(new NoOpReduceFunction())
				.addSink(new NoOpSinkFunction())
				.name("sink0").uid("sink0");

		src.map(new NoOpMapFunction())
				.keyBy(new NoOpKeySelector())
				.reduce(new NoOpReduceFunction())
				.addSink(new NoOpSinkFunction())
				.name("sink1").uid("sink1");

		JobGraph newJobGraph = env.getStreamGraph().getJobGraph();
		assertNotEquals(jobGraph.getJobID(), newJobGraph.getJobID());

		for (JobVertex vertex : newJobGraph.getVertices()) {
			// Verify that the expected IDs are the same
			if (vertex.getName().endsWith("source")
					|| vertex.getName().endsWith("sink0")
					|| vertex.getName().endsWith("sink1")) {

				assertTrue(ids.contains(vertex.getID()));
			}
		}
	}

	/**
	 * Tests that a collision on the manual hash throws an Exception.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testManualHashAssignmentCollisionThrowsException() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.disableOperatorChaining();

		env.addSource(new NoOpSourceFunction()).uid("source")
				.map(new NoOpMapFunction()).uid("source") // Collision
				.addSink(new NoOpSinkFunction());

		// This call is necessary to generate the job graph
		env.getStreamGraph().getJobGraph();
	}

	/**
	 * Tests that a manual hash for an intermediate chain node throws an Exception.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testManualHashAssignmentForIntermediateNodeInChainThrowsException() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction())
				// Intermediate chained node
				.map(new NoOpMapFunction()).uid("map")
				.addSink(new NoOpSinkFunction());

		env.getStreamGraph().getJobGraph();
	}

	/**
	 * Tests that a manual hash at the beginning of a chain is accepted.
	 *
	 * <p>This should work, because the ID is used at the beginning of a chain. This is currently
	 * not allowed for intermediate nodes (see {@link #testManualHashAssignmentForIntermediateNodeInChainThrowsException()}).
	 */
	@Test
	public void testManualHashAssignmentForStartNodeInInChain() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);

		env.addSource(new NoOpSourceFunction()).uid("source")
				.map(new NoOpMapFunction())
				.addSink(new NoOpSinkFunction());

		env.getStreamGraph().getJobGraph();
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns a {@link JobVertexID} to vertex name mapping for the given graph.
	 */
	private Map<JobVertexID, String> rememberIds(JobGraph jobGraph) {
		final Map<JobVertexID, String> ids = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			ids.put(vertex.getID(), vertex.getName());
		}
		return ids;
	}

	/**
	 * Verifies that each {@link JobVertexID} of the {@link JobGraph} is contained in the given map
	 * and mapped to the same vertex name.
	 */
	private void verifyIdsEqual(JobGraph jobGraph, Map<JobVertexID, String> ids) {
		// Verify same number of vertices
		assertEquals(jobGraph.getNumberOfVertices(), ids.size());

		// Verify that all IDs->name mappings are identical
		for (JobVertex vertex : jobGraph.getVertices()) {
			String expectedName = ids.get(vertex.getID());
			assertNotNull(expectedName);
			assertEquals(expectedName, vertex.getName());
		}
	}

	/**
	 * Verifies that no {@link JobVertexID} of the {@link JobGraph} is contained in the given map.
	 */
	private void verifyIdsNotEqual(JobGraph jobGraph, Map<JobVertexID, String> ids) {
		// Verify same number of vertices
		assertEquals(jobGraph.getNumberOfVertices(), ids.size());

		// Verify that all IDs->name mappings are identical
		for (JobVertex vertex : jobGraph.getVertices()) {
			assertFalse(ids.containsKey(vertex.getID()));
		}
	}

	// ------------------------------------------------------------------------

	private static class NoOpSourceFunction implements ParallelSourceFunction<String> {

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}

	private static class NoOpSinkFunction implements SinkFunction<String> {

		@Override
		public void invoke(String value) throws Exception {
		}
	}

	private static class NoOpMapFunction implements MapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

	private static class NoOpFilterFunction implements FilterFunction<String> {

		@Override
		public boolean filter(String value) throws Exception {
			return true;
		}
	}

	private static class NoOpKeySelector implements KeySelector<String, String> {

		@Override
		public String getKey(String value) throws Exception {
			return value;
		}
	}

	private static class NoOpReduceFunction implements ReduceFunction<String> {
		@Override
		public String reduce(String value1, String value2) throws Exception {
			return value1;
		}
	}

}
