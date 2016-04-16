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

package org.apache.flink.graph.asm;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.CompleteGraph;
import org.apache.flink.graph.generator.EmptyGraph;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Before;

import java.util.LinkedList;
import java.util.List;

public class AsmTestBase {

	protected ExecutionEnvironment env;

	// simple graph
	protected Graph<IntValue,NullValue,NullValue> directedSimpleGraph;

	protected Graph<IntValue,NullValue,NullValue> undirectedSimpleGraph;

	// complete graph
	protected final long completeGraphVertexCount = 47;

	protected Graph<LongValue,NullValue,NullValue> completeGraph;

	// empty graph
	protected Graph<LongValue,NullValue,NullValue> emptyGraph;

	// RMat graph
	protected Graph<LongValue,NullValue,NullValue> directedRMatGraph;

	protected Graph<LongValue,NullValue,NullValue> undirectedRMatGraph;

	@Before
	public void setup() {
		env = ExecutionEnvironment.createCollectionsEnvironment();

		// the "fish" graph
		Object[][] edges = new Object[][] {
			new Object[]{0, 1},
			new Object[]{0, 2},
			new Object[]{1, 2},
			new Object[]{1, 3},
			new Object[]{2, 3},
			new Object[]{3, 4},
			new Object[]{3, 5},
		};

		List<Edge<IntValue,NullValue>> directedEdgeList = new LinkedList<>();

		for (Object[] edge : edges) {
			directedEdgeList.add(new Edge<>(new IntValue((int) edge[0]), new IntValue((int) edge[1]), NullValue.getInstance()));
		}

		directedSimpleGraph = Graph.fromCollection(directedEdgeList, env);
		undirectedSimpleGraph = directedSimpleGraph
			.getUndirected();

		// complete graph
		completeGraph = new CompleteGraph(env, completeGraphVertexCount)
			.generate();

		// empty graph
		emptyGraph = new EmptyGraph(env, 3)
			.generate();

		// RMat graph
		long rmatVertexCount = 1 << 10;
		long rmatEdgeCount = 16 * rmatVertexCount;

		directedRMatGraph = new RMatGraph<>(env, new JDKRandomGeneratorFactory(), rmatVertexCount, rmatEdgeCount)
			.generate();

		undirectedRMatGraph = new RMatGraph<>(env, new JDKRandomGeneratorFactory(), rmatVertexCount, rmatEdgeCount)
			.setSimpleGraph(true, false)
			.generate();
	}
}
