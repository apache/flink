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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Objects;

/**
 * Tests for {@link Graph#groupReduceOnEdges} and {@link Graph#reduceOnEdges}.
 */
@RunWith(Parameterized.class)
public class ReduceOnEdgesMethodsITCase extends MultipleProgramsTestBase {

	public ReduceOnEdgesMethodsITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String expectedResult;

	@Test
	public void testLowestWeightOutNeighbor() throws Exception {
		/*
		 * Get the lowest-weight out-neighbor
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.groupReduceOnEdges(new SelectMinWeightNeighbor(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,2\n" +
			"2,3\n" +
			"3,4\n" +
			"4,5\n" +
			"5,1\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testLowestWeightInNeighbor() throws Exception {
		/*
		 * Get the lowest-weight in-neighbor
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.groupReduceOnEdges(new SelectMinWeightInNeighbor(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"4,3\n" +
			"5,3\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighbors() throws Exception {
		/*
		 * Get the all the out-neighbors for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighbors(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"2,3\n" +
			"3,4\n" +
			"3,5\n" +
			"4,5\n" +
			"5,1";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighborsNoValue() throws Exception {
		/*
		 * Get the all the out-neighbors for each vertex except for the vertex with id 5.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighborsExcludeFive(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"2,3\n" +
			"3,4\n" +
			"3,5\n" +
			"4,5";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllOutNeighborsWithValueGreaterThanTwo() throws Exception {
		/*
		 * Get the all the out-neighbors for each vertex that have a value greater than two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllOutNeighbors =
			graph.groupReduceOnEdges(new SelectOutNeighborsValueGreaterThanTwo(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithAllOutNeighbors.collect();

		expectedResult = "3,4\n" +
			"3,5\n" +
			"4,5\n" +
			"5,1";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighbors() throws Exception {
		/*
		 * Get the all the in-neighbors for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighbors(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"3,2\n" +
			"4,3\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighborsNoValue() throws Exception {
		/*
		 * Get the all the in-neighbors for each vertex except for the vertex with id 5.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighborsExceptFive(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "1,5\n" +
			"2,1\n" +
			"3,1\n" +
			"3,2\n" +
			"4,3";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllInNeighborsWithValueGreaterThanTwo() throws Exception {
		/*
		 * Get the all the in-neighbors for each vertex that have a value greater than two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllInNeighbors =
			graph.groupReduceOnEdges(new SelectInNeighborsValueGreaterThanTwo(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithAllInNeighbors.collect();

		expectedResult = "3,1\n" +
			"3,2\n" +
			"4,3\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighbors() throws Exception {
		/*
		 * Get the all the neighbors for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighbors(), EdgeDirection.ALL);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"1,5\n" +
			"2,1\n" +
			"2,3\n" +
			"3,1\n" +
			"3,2\n" +
			"3,4\n" +
			"3,5\n" +
			"4,3\n" +
			"4,5\n" +
			"5,1\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighborsNoValue() throws Exception {
		/*
		 * Get the all the neighbors for each vertex except for vertices with id 5 and 2.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighborsExceptFiveAndTwo(), EdgeDirection.ALL);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "1,2\n" +
			"1,3\n" +
			"1,5\n" +
			"3,1\n" +
			"3,2\n" +
			"3,4\n" +
			"3,5\n" +
			"4,3\n" +
			"4,5";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testAllNeighborsWithValueGreaterThanFour() throws Exception {
		/*
		 * Get the all the neighbors for each vertex that have a value greater than four.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithAllNeighbors =
			graph.groupReduceOnEdges(new SelectNeighborsValueGreaterThanFour(), EdgeDirection.ALL);
		List<Tuple2<Long, Long>> result = verticesWithAllNeighbors.collect();

		expectedResult = "5,1\n" +
			"5,3\n" +
			"5,4";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testMaxWeightEdge() throws Exception {
		/*
		 * Get the maximum weight among all edges
		 * of a vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight =
			graph.groupReduceOnEdges(new SelectMaxWeightNeighbor(), EdgeDirection.ALL);
		List<Tuple2<Long, Long>> result = verticesWithMaxEdgeWeight.collect();

		expectedResult = "1,51\n" +
			"2,23\n" +
			"3,35\n" +
			"4,45\n" +
			"5,51\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testLowestWeightOutNeighborNoValue() throws Exception {
		/*
		 * Get the lowest-weight out of all the out-neighbors
		 * of each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.reduceOnEdges(new SelectMinWeightNeighborNoValue(), EdgeDirection.OUT);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,12\n" +
			"2,23\n" +
			"3,34\n" +
			"4,45\n" +
			"5,51\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testLowestWeightInNeighborNoValue() throws Exception {
		/*
		 * Get the lowest-weight out of all the in-neighbors
		 * of each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor =
			graph.reduceOnEdges(new SelectMinWeightNeighborNoValue(), EdgeDirection.IN);
		List<Tuple2<Long, Long>> result = verticesWithLowestOutNeighbor.collect();

		expectedResult = "1,51\n" +
			"2,12\n" +
			"3,13\n" +
			"4,34\n" +
			"5,35\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testMaxWeightAllNeighbors() throws Exception {
		/*
		 * Get the maximum weight among all edges
		 * of a vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight =
			graph.reduceOnEdges(new SelectMaxWeightNeighborNoValue(), EdgeDirection.ALL);
		List<Tuple2<Long, Long>> result = verticesWithMaxEdgeWeight.collect();

		expectedResult = "1,51\n" +
			"2,23\n" +
			"3,35\n" +
			"4,45\n" +
			"5,51\n";

		compareResultAsTuples(result, expectedResult);
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MAX_VALUE;
			long minNeighborId = 0;

			for (Edge<Long, Long> edge : edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighborId = edge.getTarget();
				}
			}
			out.collect(new Tuple2<>(v.getId(), minNeighborId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MIN_VALUE;

			for (Edge<Long, Long> edge : edges) {
				if (edge.getValue() > weight) {
					weight = edge.getValue();
				}
			}
			out.collect(new Tuple2<>(v.getId(), weight));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightNeighborNoValue implements ReduceEdgesFunction<Long> {

		@Override
		public Long reduceEdges(Long firstEdgeValue, Long secondEdgeValue) {
			return Math.min(firstEdgeValue, secondEdgeValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighborNoValue implements ReduceEdgesFunction<Long> {

		@Override
		public Long reduceEdges(Long firstEdgeValue, Long secondEdgeValue) {
			return Math.max(firstEdgeValue, secondEdgeValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

			long weight = Long.MAX_VALUE;
			long minNeighborId = 0;

			for (Edge<Long, Long> edge : edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighborId = edge.getSource();
				}
			}
			out.collect(new Tuple2<>(v.getId(), minNeighborId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<>(edge.f0, edge.f1.getTarget()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighborsExcludeFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.getTarget()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighborsValueGreaterThanTwo implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.getValue() > 2) {
					out.collect(new Tuple2<>(v.getId(), edge.getTarget()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<>(edge.f0, edge.f1.getSource()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighborsExceptFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.getSource()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighborsValueGreaterThanTwo implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.getValue() > 2) {
					out.collect(new Tuple2<>(v.getId(), edge.getSource()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (Objects.equals(edge.f0, edge.f1.getTarget())) {
					out.collect(new Tuple2<>(edge.f0, edge.f1.getSource()));
				} else {
					out.collect(new Tuple2<>(edge.f0, edge.f1.getTarget()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighborsExceptFiveAndTwo implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if (edge.f0 != 5 && edge.f0 != 2) {
					if (Objects.equals(edge.f0, edge.f1.getTarget())) {
						out.collect(new Tuple2<>(edge.f0, edge.f1.getSource()));
					} else {
						out.collect(new Tuple2<>(edge.f0, edge.f1.getTarget()));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectNeighborsValueGreaterThanFour implements
		EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v, Iterable<Edge<Long, Long>> edges,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			for (Edge<Long, Long> edge : edges) {
				if (v.getValue() > 4) {
					if (v.getId().equals(edge.getTarget())) {
						out.collect(new Tuple2<>(v.getId(), edge.getSource()));
					} else {
						out.collect(new Tuple2<>(v.getId(), edge.getTarget()));
					}
				}
			}
		}
	}
}
