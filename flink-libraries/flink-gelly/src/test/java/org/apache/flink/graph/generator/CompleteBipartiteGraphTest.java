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
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.graph.bipartite.generator.CompleteBipartiteGraph;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.util.List;

public class CompleteBipartiteGraphTest extends AbstractGraphTest {

	@Test
	public void testGenerateCompleteGraph() throws Exception {
		BipartiteGraph<LongValue, LongValue, NullValue, NullValue, NullValue> graph
			= new CompleteBipartiteGraph(env, 2, 3)
			.generate();


		List<Vertex<LongValue, NullValue>> topVertices = graph.getTopVertices().collect();
		List<Vertex<LongValue, NullValue>> bottomVertices = graph.getBottomVertices().collect();
		List<BipartiteEdge<LongValue, LongValue, NullValue>> edges = graph.getEdges().collect();

		TestBaseUtils.compareResultAsText(topVertices,
			"(0,(null))\n" +
			"(1,(null))");
		TestBaseUtils.compareResultAsText(bottomVertices,
			"(0,(null))\n" +
			"(1,(null))\n" +
            "(2,(null))");
		TestBaseUtils.compareResultAsText(edges,
			"(0,0,(null))\n" +
			"(0,1,(null))\n" +
            "(0,2,(null))\n" +
			"(1,0,(null))\n" +
			"(1,1,(null))\n" +
			"(1,2,(null))\n");
	}

	@Test
	public void testParallelism() {
		int parallelism = 10;
		BipartiteGraph<LongValue, LongValue, NullValue, NullValue, NullValue> graph
			= new CompleteBipartiteGraph(env, 200, 300)
			.setParallelism(parallelism)
			.generate();

		graph.getTopVertices().output(new DiscardingOutputFormat<Vertex<LongValue, NullValue>>());
		graph.getBottomVertices().output(new DiscardingOutputFormat<Vertex<LongValue, NullValue>>());
		graph.getEdges().output(new DiscardingOutputFormat<BipartiteEdge<LongValue, LongValue, NullValue>>());

		TestUtils.verifyParallelism(env, 10);
	}

}
