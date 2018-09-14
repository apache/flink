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

package org.apache.flink.graph.generator;

import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link EchoGraph}.
 */
public class EchoGraphTest extends GraphGeneratorTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGraphWithEvenVertexCountWithOddVertexDegree() throws Exception {
		Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, 10, 3)
			.generate();

		String vertices = "0; 1; 2; 3; 4; 5; 6; 7; 8; 9";
		String edges = "0,4; 0,5; 0,6; 1,5; 1,6; 1,7; 2,6;" +
			"2,7; 2,8; 3,7; 3,8; 3,9; 4,0; 4,8; 4,9;" +
			"5,0; 5,1; 5,9; 6,0; 6,1; 6,2; 7,1; 7,2; 7,3;" +
			"8,2; 8,3; 8,4; 9,3; 9,4; 9,5";

		TestUtils.compareGraph(graph, vertices, edges);
	}

	@Test
	public void testGraphWithOddVertexCountWithEvenVertexDegree() throws Exception {
		Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, 9, 2)
			.generate();

		String vertices = "0; 1; 2; 3; 4; 5; 6; 7; 8";
		String edges = "0,4; 0,5; 1,5; 1,6; 2,6; 2,7;" +
			"3,7; 3,8; 4,8; 4,0; 5,0; 5,1;" +
			"6,1; 6,2; 7,2; 7,3; 8,3; 8,4";

		TestUtils.compareGraph(graph, vertices, edges);
	}

	@Test
	public void testGraphWithOddVertexCountWithOddVertexDegree() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Vertex count or vertex degree must be an even number but not both.");

		new EchoGraph(env, 5, 3).generate();
	}

	@Test
	public void testGraphWithEvenVertexCountWithEvenVertexDegree() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Vertex count or vertex degree must be an even number but not both.");

		new EchoGraph(env, 6, 2).generate();
	}

	@Test
	public void testGraphWithVertexDegreeTooLarge() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Vertex degree must be less than the vertex count.");

		new EchoGraph(env, 8, 8).generate();
	}

	@Test
	public void testGraphMetrics() throws Exception {
		int vertexCount = 10;
		int vertexDegree = 3;

		Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, vertexCount, vertexDegree)
			.generate();

		assertEquals(vertexCount, graph.numberOfVertices());
		assertEquals(vertexCount * vertexDegree, graph.numberOfEdges());

		long maxInDegree = graph.inDegrees().max(1).collect().get(0).f1.getValue();
		long maxOutDegree = graph.outDegrees().max(1).collect().get(0).f1.getValue();

		assertEquals(vertexDegree, maxInDegree);
		assertEquals(vertexDegree, maxOutDegree);
	}

	@Test
	public void testParallelism() throws Exception {
		int parallelism = 2;

		Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, 10, 3)
			.setParallelism(parallelism)
			.generate();

		graph.getVertices().output(new DiscardingOutputFormat<>());
		graph.getEdges().output(new DiscardingOutputFormat<>());

		TestUtils.verifyParallelism(env, parallelism);
	}
}
