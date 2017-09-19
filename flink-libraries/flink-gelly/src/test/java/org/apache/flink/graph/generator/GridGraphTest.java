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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GridGraph}.
 */
public class GridGraphTest extends GraphGeneratorTestBase {

	@Test
	public void testGraph() throws Exception {
		Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
			.addDimension(2, false)
			.addDimension(3, false)
			.generate();

		// 0 1 2
		// 3 4 5
		String vertices = "0; 1; 2; 3; 4; 5";
		String edges = "0,1; 0,3; 1,0; 1,2; 1,4; 2,1; 2,5; 3,0; 3,4; 4,1;" +
			"4,3; 4,5; 5,2; 5,4";

		TestUtils.compareGraph(graph, vertices, edges);
	}

	@Test
	public void testGraphMetrics() throws Exception {
		Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
			.addDimension(2, true)
			.addDimension(3, true)
			.addDimension(5, true)
			.addDimension(7, true)
			.generate();

		// Each vertex is the source of one edge in the first dimension of size 2,
		// and the source of two edges in each dimension of size greater than 2.
		assertEquals(2 * 3 * 5 * 7, graph.numberOfVertices());
		assertEquals(7 * 2 * 3 * 5 * 7, graph.numberOfEdges());

		long minInDegree = graph.inDegrees().min(1).collect().get(0).f1.getValue();
		long minOutDegree = graph.outDegrees().min(1).collect().get(0).f1.getValue();
		long maxInDegree = graph.inDegrees().max(1).collect().get(0).f1.getValue();
		long maxOutDegree = graph.outDegrees().max(1).collect().get(0).f1.getValue();

		assertEquals(7, minInDegree);
		assertEquals(7, minOutDegree);
		assertEquals(7, maxInDegree);
		assertEquals(7, maxOutDegree);
	}

	@Test
	public void testParallelism() throws Exception {
		int parallelism = 2;

		Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
			.addDimension(3, false)
			.addDimension(5, false)
			.setParallelism(parallelism)
			.generate();

		graph.getVertices().output(new DiscardingOutputFormat<>());
		graph.getEdges().output(new DiscardingOutputFormat<>());

		TestUtils.verifyParallelism(env, parallelism);
	}
}
