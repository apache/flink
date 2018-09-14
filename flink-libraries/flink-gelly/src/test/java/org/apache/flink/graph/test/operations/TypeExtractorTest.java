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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test output types from {@link Graph} methods.
 */
public class TypeExtractorTest {

	private Graph<Long, Long, Long> inputGraph;
	private DataSet<Vertex<Long, Long>> vertices;
	private DataSet<Edge<Long, Long>> edges;
	private ExecutionEnvironment env;

	@Before
	public void setUp() throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
		vertices = TestGraphUtils.getLongLongVertexData(env);
		edges = TestGraphUtils.getLongLongEdgeData(env);
		inputGraph = Graph.fromDataSet(vertices, edges, env);
	}

	@Test
	public void testMapVerticesType() throws Exception {

		// test type extraction in mapVertices
		DataSet<Vertex<Long, Tuple2<Long, Integer>>> outVertices = inputGraph.mapVertices(new VertexMapper<>()).getVertices();
		Assert.assertTrue(new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
			new TupleTypeInfo<Tuple2<Long, Integer>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
			.equals(outVertices.getType()));
	}

	@Test
	public void testMapEdgesType() throws Exception {

		// test type extraction in mapEdges
		DataSet<Edge<Long, Tuple2<Long, Integer>>> outEdges = inputGraph.mapEdges(new EdgeMapper<>()).getEdges();
		Assert.assertTrue(new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,
			new TupleTypeInfo<Tuple2<Long, Integer>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
			.equals(outEdges.getType()));
	}

	@Test
	public void testFromDataSet() throws Exception {
		DataSet<Vertex<Long, Tuple2<Long, Integer>>> outVertices = Graph.fromDataSet(edges, new VertexInitializer<>(), env)
			.getVertices();
		Assert.assertTrue(new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
			new TupleTypeInfo<Tuple2<Long, Integer>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
			.equals(outVertices.getType()));
	}

	@Test
	public void testGroupReduceOnEdges() throws Exception {
		DataSet<Tuple2<Long, Long>> output = inputGraph.groupReduceOnEdges(new EdgesGroupFunction<>(), EdgeDirection.OUT);
		Assert.assertTrue((new TupleTypeInfo<Tuple2<Long, Long>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)).equals(output.getType()));
	}

	private static final class VertexMapper<K> implements MapFunction<Vertex<K, Long>, Tuple2<K, Integer>> {

		private final Tuple2<K, Integer> outTuple = new Tuple2<>();

		@Override
		public Tuple2<K, Integer> map(Vertex<K, Long> inputVertex) throws Exception {
			return outTuple;
		}
	}

	private static final class EdgeMapper<K> implements MapFunction<Edge<K, Long>, Tuple2<K, Integer>> {

		private final Tuple2<K, Integer> outTuple = new Tuple2<>();

		@Override
		public Tuple2<K, Integer> map(Edge<K, Long> inputEdge) throws Exception {
			return outTuple;
		}
	}

	private static final class EdgesGroupFunction<K, EV> implements EdgesFunction<K, EV, Tuple2<K, EV>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<K, Edge<K, EV>>> edges, Collector<Tuple2<K, EV>> out) throws Exception {
			out.collect(new Tuple2<>());
		}
	}

	private static final class VertexInitializer<K> implements MapFunction<K, Tuple2<K, Integer>> {

		@Override
		public Tuple2<K, Integer> map(K value) throws Exception {
			return null;
		}
	}
}
