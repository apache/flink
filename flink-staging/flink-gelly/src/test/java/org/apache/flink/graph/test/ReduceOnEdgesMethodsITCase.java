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

package org.apache.flink.graph.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ReduceOnEdgesMethodsITCase extends MultipleProgramsTestBase {

	public ReduceOnEdgesMethodsITCase(MultipleProgramsTestBase.ExecutionMode mode){
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
				graph.reduceOnEdges(new SelectMinWeightNeighbor(), EdgeDirection.OUT);
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
				graph.reduceOnEdges(new SelectMinWeightInNeighbor(), EdgeDirection.IN);
		verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
					"2,1\n" + 
					"3,1\n" +
					"4,3\n" + 
					"5,3\n";
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
				graph.reduceOnEdges(new SelectMaxWeightNeighbor(), EdgeDirection.ALL);
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
				graph.reduceOnEdges(new SelectMinWeightInNeighborNoValue(), EdgeDirection.IN);
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

		public Tuple2<Long, Long> iterateEdges(
				Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges) {
			
			long weight = Long.MAX_VALUE;
			long minNeighorId = 0;
			
			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighorId = edge.getTarget();
				}
			}
			return new Tuple2<Long, Long>(v.getId(), minNeighorId);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> iterateEdges(Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges) {
			
			long weight = Long.MIN_VALUE;

			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() > weight) {
					weight = edge.getValue();
				}
			}
			return new Tuple2<Long, Long>(v.getId(), weight);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightNeighborNoValue implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {

			long weight = Long.MAX_VALUE;
			long minNeighorId = 0;
			long vertexId = -1;
			long i=0;

			for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
				if (edge.f1.getValue() < weight) {
					weight = edge.f1.getValue();
					minNeighorId = edge.f1.getTarget();
				}
				if (i==0) {
					vertexId = edge.f0;
				} i++;
			}
			return new Tuple2<Long, Long>(vertexId, minNeighorId);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMaxWeightNeighborNoValue implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {
			
			long weight = Long.MIN_VALUE;
			long vertexId = -1;
			long i=0;

			for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
				if (edge.f1.getValue() > weight) {
					weight = edge.f1.getValue();
				}
				if (i==0) {
					vertexId = edge.f0;
				} i++;
			}
			return new Tuple2<Long, Long>(vertexId, weight);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighbor implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> iterateEdges(
				Vertex<Long, Long> v,
				Iterable<Edge<Long, Long>> edges) {
			
			long weight = Long.MAX_VALUE;
			long minNeighorId = 0;
			
			for (Edge<Long, Long> edge: edges) {
				if (edge.getValue() < weight) {
					weight = edge.getValue();
					minNeighorId = edge.getSource();
				}
			}
			return new Tuple2<Long, Long>(v.getId(), minNeighorId);
		}
	}

	@SuppressWarnings("serial")
	private static final class SelectMinWeightInNeighborNoValue implements EdgesFunction<Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {
			
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
			return new Tuple2<Long, Long>(vertexId, minNeighorId);
		}
	}
}