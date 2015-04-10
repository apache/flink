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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ReduceOnEdgesMethodsITCase extends MultipleProgramsTestBase {

	public ReduceOnEdgesMethodsITCase(TestExecutionMode mode){
		super(mode);
	}

    private String resultPath;
    private String expectedResult;

    @Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

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
		verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
		env.execute();
	
		expectedResult = "1,2\n" +
				"2,3\n" + 
				"3,4\n" +
				"4,5\n" + 
				"5,1\n";
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
		verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
					"2,1\n" + 
					"3,1\n" +
					"4,3\n" + 
					"5,3\n";
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
		verticesWithAllOutNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2\n" +
				"1,3\n" +
				"2,3\n" +
				"3,4\n" +
				"3,5\n" +
				"4,5\n" +
				"5,1";
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
		verticesWithAllOutNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2\n" +
				"1,3\n" +
				"2,3\n" +
				"3,4\n" +
				"3,5\n" +
				"4,5";
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
		verticesWithAllOutNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,4\n" +
				"3,5\n" +
				"4,5\n" +
				"5,1";
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
		verticesWithAllInNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
				"2,1\n" +
				"3,1\n" +
				"3,2\n" +
				"4,3\n" +
				"5,3\n" +
				"5,4";
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
		verticesWithAllInNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
				"2,1\n" +
				"3,1\n" +
				"3,2\n" +
				"4,3";
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
		verticesWithAllInNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,1\n" +
				"3,2\n" +
				"4,3\n" +
				"5,3\n" +
				"5,4";
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
		verticesWithAllNeighbors.writeAsCsv(resultPath);
		env.execute();

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
		verticesWithAllNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2\n" +
				"1,3\n" +
				"1,5\n" +
				"3,1\n" +
				"3,2\n" +
				"3,4\n" +
				"3,5\n" +
				"4,3\n" +
				"4,5";
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
		verticesWithAllNeighbors.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "5,1\n" +
				"5,3\n" +
				"5,4";
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
		verticesWithMaxEdgeWeight.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,51\n" +
				"2,23\n" + 
				"3,35\n" +
				"4,45\n" + 
				"5,51\n";
	}

	@Test
	public void testLowestWeightOutNeighborNoValue() throws Exception {
		/*
		 * Get the lowest-weight out-neighbor
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
				graph.reduceOnEdges(new SelectMinWeightNeighborNoValue(), EdgeDirection.OUT);
		verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,2\n" +
				"2,3\n" + 
				"3,4\n" +
				"4,5\n" + 
				"5,1\n";
	}

	@Test
	public void testLowestWeightInNeighborNoValue() throws Exception {
		/*
		 * Get the lowest-weight in-neighbor
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
				graph.groupReduceOnEdges(new SelectMinWeightInNeighborNoValue(), EdgeDirection.IN);
		verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
				"2,1\n" + 
				"3,1\n" +
				"4,3\n" + 
				"5,3\n";
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
		verticesWithMaxEdgeWeight.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,51\n" +
				"2,23\n" + 
				"3,35\n" +
				"4,45\n" + 
				"5,51\n";
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {
			
			long weight = Long.MAX_VALUE;
			long minNeighborId = 0;

			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighborId = edge.getTarget();
				}
			}
			out.collect(new Tuple2<Long, Long>(v.getId(), minNeighborId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {
			
			long weight = Long.MIN_VALUE;

			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() > weight) {
					weight = edge.getValue();
				}
			}
			out.collect(new Tuple2<Long, Long>(v.getId(), weight));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightNeighborNoValue implements ReduceEdgesFunction<Long, Long> {

		@Override
		public Tuple2<Long, Edge<Long, Long>> reduceEdges(Tuple2<Long, Edge<Long, Long>> firstEdge,
														  Tuple2<Long, Edge<Long, Long>> secondEdge) {

			if(firstEdge.f1.getValue() < secondEdge.f1.getValue()) {
				return firstEdge;
			} else {
				return secondEdge;
			}

		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighborNoValue implements ReduceEdgesFunction<Long, Long> {

//		@Override
//		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
//								 Collector<Tuple2<Long, Long>> out) throws Exception {
//
//			long weight = Long.MIN_VALUE;
//			long vertexId = -1;
//			long i=0;
//
//			for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
//				if (edge.f1.getValue() > weight) {
//					weight = edge.f1.getValue();
//				}
//				if (i==0) {
//					vertexId = edge.f0;
//				} i++;
//			}
//			out.collect(new Tuple2<Long, Long>(vertexId, weight));
//		}

		@Override
		public Tuple2<Long, Edge<Long, Long>> reduceEdges(Tuple2<Long, Edge<Long, Long>> firstEdge,
														  Tuple2<Long, Edge<Long, Long>> secondEdge) {
			if(firstEdge.f1.getValue() > secondEdge.f1.getValue()) {
				return firstEdge;
			} else {
				return secondEdge;
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {
			
			long weight = Long.MAX_VALUE;
			long minNeighorId = 0;
			
			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighorId = edge.getSource();
				}
			}
			out.collect(new Tuple2<Long, Long>(v.getId(), minNeighorId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighborNoValue implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
								 Collector<Tuple2<Long, Long>> out) throws Exception {
			
			long weight = Long.MAX_VALUE;
			long minNeighorId = 0;
			long vertexId = -1;
			long i=0;

			for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
				if (edge.f1.getValue() < weight) {
					weight = edge.f1.getValue();
					minNeighorId = edge.f1.getSource();
				}
				if (i==0) {
					vertexId = edge.f0;
				} i++;
			}
			out.collect(new Tuple2<Long, Long>(vertexId, minNeighorId));
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
								 Collector<Tuple2<Long, Long>> out) throws Exception {

			for(Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getTarget()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectOutNeighborsExcludeFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
								 Collector<Tuple2<Long, Long>> out) throws Exception {

			for(Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if(edge.f0 != 5) {
					out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getTarget()));
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
			for (Edge<Long, Long> edge: edges) {
				if(v.getValue() > 2) {
					out.collect(new Tuple2<Long, Long>(v.getId(), edge.getTarget()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighbors implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
								 Collector<Tuple2<Long, Long>> out) throws Exception {

			for(Tuple2<Long, Edge<Long, Long>> edge : edges) {
				out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getSource()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectInNeighborsExceptFive implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		@Override
		public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges,
								 Collector<Tuple2<Long, Long>> out) throws Exception {

			for(Tuple2<Long, Edge<Long, Long>> edge : edges) {
				if(edge.f0 != 5) {
					out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getSource()));
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
			for (Edge<Long, Long> edge: edges) {
				if(v.getValue() > 2) {
					out.collect(new Tuple2<Long, Long>(v.getId(), edge.getSource()));
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
				if (edge.f0 == edge.f1.getTarget()) {
					out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getSource()));
				} else {
					out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getTarget()));
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
				if(edge.f0 != 5 && edge.f0 != 2) {
					if (edge.f0 == edge.f1.getTarget()) {
						out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getSource()));
					} else {
						out.collect(new Tuple2<Long, Long>(edge.f0, edge.f1.getTarget()));
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
			for(Edge<Long, Long> edge : edges) {
				if(v.getValue() > 4) {
					if(v.getId().equals(edge.getTarget())) {
						out.collect(new Tuple2<Long, Long>(v.getId(), edge.getSource()));
					} else {
						out.collect(new Tuple2<Long, Long>(v.getId(), edge.getTarget()));
					}
				}
			}
		}
	}
}