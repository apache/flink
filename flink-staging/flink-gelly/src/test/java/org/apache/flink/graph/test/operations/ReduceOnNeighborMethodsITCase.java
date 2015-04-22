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

import java.util.Iterator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunction;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.ReduceNeighborsFunction;
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
public class ReduceOnNeighborMethodsITCase extends MultipleProgramsTestBase {

	public ReduceOnNeighborMethodsITCase(TestExecutionMode mode){
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
	public void testSumOfOutNeighbors() throws Exception {
		/*
		 * Get the sum of out-neighbor values
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues = 
				graph.groupReduceOnNeighbors(new SumOutNeighbors(), EdgeDirection.OUT);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,5\n" +
				"2,3\n" + 
				"3,9\n" +
				"4,5\n" + 
				"5,1\n";
	}

	@Test
	public void testSumOfInNeighbors() throws Exception {
		/*
		 * Get the sum of in-neighbor values
		 * times the edge weights for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSum = 
				graph.groupReduceOnNeighbors(new SumInNeighbors(), EdgeDirection.IN);

		verticesWithSum.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,255\n" +
				"2,12\n" + 
				"3,59\n" +
				"4,102\n" + 
				"5,285\n";
	}

	@Test
	public void testSumOfOAllNeighbors() throws Exception {
		/*
		 * Get the sum of all neighbor values
		 * including own vertex value
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues = 
				graph.groupReduceOnNeighbors(new SumAllNeighbors(), EdgeDirection.ALL);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,11\n" +
				"2,6\n" + 
				"3,15\n" +
				"4,12\n" + 
				"5,13\n";
	}

	@Test
	public void testSumOfOutNeighborsIdGreaterThanThree() throws Exception {
		/*
		 * Get the sum of out-neighbor values
		 * for each vertex with id greater than three.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumOutNeighborsIdGreaterThanThree(), EdgeDirection.OUT);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "4,5\n" +
				"5,1\n";
	}

	@Test
	public void testSumOfInNeighborsIdGreaterThanThree() throws Exception {
		/*
		 * Get the sum of in-neighbor values
		 * times the edge weights for each vertex with id greater than three.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSum =
				graph.groupReduceOnNeighbors(new SumInNeighborsIdGreaterThanThree(), EdgeDirection.IN);

		verticesWithSum.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "4,102\n" +
				"5,285\n";
	}

	@Test
	public void testSumOfOAllNeighborsIdGreaterThanThree() throws Exception {
		/*
		 * Get the sum of all neighbor values
		 * including own vertex value
		 * for each vertex with id greater than three.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumAllNeighborsIdGreaterThanThree(), EdgeDirection.ALL);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "4,12\n" +
				"5,13\n";
	}

	@Test
	public void testSumOfOutNeighborsNoValue() throws Exception {
		/*
		 * Get the sum of out-neighbor values
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues = 
				graph.reduceOnNeighbors(new SumOutNeighborsNoValue(), EdgeDirection.OUT);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,5\n" +
				"2,3\n" + 
				"3,9\n" +
				"4,5\n" + 
				"5,1\n";
	}

	@Test
	public void testSumOfInNeighborsNoValue() throws Exception {
		/*
		 * Get the sum of in-neighbor values
		 * times the edge weights for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSum = 
				graph.groupReduceOnNeighbors(new SumInNeighborsNoValue(), EdgeDirection.IN);
		verticesWithSum.writeAsCsv(resultPath);
		env.execute();
	
		expectedResult = "1,255\n" +
				"2,12\n" +
				"3,59\n" +
				"4,102\n" +
				"5,285\n";
	}

	@Test
	public void testSumOfAllNeighborsNoValue() throws Exception {
		/*
		 * Get the sum of all neighbor values
		 * for each vertex
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), 
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfAllNeighborValues =
				graph.reduceOnNeighbors(new SumAllNeighborsNoValue(), EdgeDirection.ALL);

		verticesWithSumOfAllNeighborValues.writeAsCsv(resultPath);
		env.execute();
	
		expectedResult = "1,10\n" +
				"2,4\n" + 
				"3,12\n" +
				"4,8\n" + 
				"5,8\n";
	}

	@Test
	public void testSumOfOutNeighborsNoValueMultipliedByTwoIdGreaterThanTwo() throws Exception {
		/*
		 * Get the sum of out-neighbor values
		 * for each vertex with id greater than two as well as the same sum multiplied by two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumOutNeighborsNoValueMultipliedByTwoIdGreaterThanTwo(), EdgeDirection.OUT);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,9\n" +
				"3,18\n" +
				"4,5\n" +
				"4,10\n" +
				"5,1\n" +
				"5,2";
	}

	@Test
	public void testSumOfInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo() throws Exception {
		/*
		 * Get the sum of in-neighbor values
		 * for each vertex with id greater than two as well as the same sum multiplied by two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo(), EdgeDirection.IN);
		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,59\n" +
				"3,118\n" +
				"4,204\n" +
				"4,102\n" +
				"5,570\n" +
				"5,285";
	}

	@Test
	public void testSumOfAllNeighborsNoValueMultipliedByTwoIdGreaterThanTwo() throws Exception {
		/*
		 * Get the sum of all neighbor values
		 * for each vertex with id greater than two as well as the same sum multiplied by two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfAllNeighborValues =
				graph.groupReduceOnNeighbors(new SumAllNeighborsNoValueMultipliedByTwoIdGreaterThanTwo(), EdgeDirection.ALL);

		verticesWithSumOfAllNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "3,12\n" +
				"3,24\n" +
				"4,8\n" +
				"4,16\n" +
				"5,8\n" +
				"5,16";
	}

	@Test
	public void testSumOfOutNeighborsMultipliedByTwo() throws Exception {
		/*
		 * Get the sum of out-neighbor values
		 * for each vertex as well as the sum of out-neighbor values multiplied by two.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumOutNeighborsMultipliedByTwo(), EdgeDirection.OUT);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,5\n" +
				"1,10\n" +
				"2,3\n" +
				"2,6\n" +
				"3,9\n" +
				"3,18\n" +
				"4,5\n" +
				"4,10\n" +
				"5,1\n" +
				"5,2";
	}

	@Test
	public void testSumOfInNeighborsSubtractOne() throws Exception {
		/*
		 * Get the sum of in-neighbor values
		 * times the edge weights for each vertex as well as the same sum minus one.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSum =
				graph.groupReduceOnNeighbors(new SumInNeighborsSubtractOne(), EdgeDirection.IN);

		verticesWithSum.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,255\n" +
				"1,254\n" +
				"2,12\n" +
				"2,11\n" +
				"3,59\n" +
				"3,58\n" +
				"4,102\n" +
				"4,101\n" +
				"5,285\n" +
				"5,284";
	}

	@Test
	public void testSumOfOAllNeighborsAddFive() throws Exception {
		/*
		 * Get the sum of all neighbor values
		 * including own vertex value
		 * for each vertex as well as the same sum plus five.
         */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
				graph.groupReduceOnNeighbors(new SumAllNeighborsAddFive(), EdgeDirection.ALL);

		verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,11\n" +
				"1,16\n" +
				"2,6\n" +
				"2,11\n" +
				"3,15\n" +
				"3,20\n" +
				"4,12\n" +
				"4,17\n" +
				"5,13\n" +
				"5,18";
	}

	@SuppressWarnings("serial")
	private static final class SumOutNeighbors implements NeighborsFunctionWithVertexValue<Long, Long, Long, 
	Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
				Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			
			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumInNeighbors implements NeighborsFunctionWithVertexValue<Long, Long, Long, 
		Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
				Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
				Collector<Tuple2<Long, Long>> out) throws Exception {
		
			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f0.getValue() * neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumAllNeighbors implements NeighborsFunctionWithVertexValue<Long, Long, Long, 
		Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {
	
			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum + vertex.getValue()));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumOutNeighborsIdGreaterThanThree implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
					sum += neighbor.f1.getValue();
			}
			if(vertex.getId() > 3) {
				out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumInNeighborsIdGreaterThanThree implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f0.getValue() * neighbor.f1.getValue();
			}
			if(vertex.getId() > 3) {
				out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumAllNeighborsIdGreaterThanThree implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f1.getValue();
			}
			if(vertex.getId() > 3) {
				out.collect(new Tuple2<Long, Long>(vertex.getId(), sum + vertex.getValue()));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumOutNeighborsNoValue implements ReduceNeighborsFunction<Long, Long> {

		@Override
		public Tuple2<Long, Long> reduceNeighbors(Tuple2<Long, Long> firstNeighbor,
												  Tuple2<Long, Long> secondNeighbor) {
			long sum = firstNeighbor.f1 + secondNeighbor.f1;
			return new Tuple2<Long, Long>(firstNeighbor.f0, sum);
		}
	}

	@SuppressWarnings("serial")
	private static final class SumInNeighborsNoValue implements NeighborsFunction<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {
			long sum = 0;
			Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> next = null;
			Iterator<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighborsIterator =
					neighbors.iterator();
			while(neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				sum += next.f2.getValue() * next.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(next.f0, sum));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumAllNeighborsNoValue implements ReduceNeighborsFunction<Long, Long> {

		@Override
		public Tuple2<Long, Long> reduceNeighbors(Tuple2<Long, Long> firstNeighbor,
												  Tuple2<Long, Long> secondNeighbor) {
			long sum = firstNeighbor.f1 + secondNeighbor.f1;
			return new Tuple2<Long, Long>(firstNeighbor.f0, sum);
		}
	}

	@SuppressWarnings("serial")
	private static final class SumOutNeighborsNoValueMultipliedByTwoIdGreaterThanTwo implements NeighborsFunction<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> next = null;
			Iterator<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighborsIterator =
					neighbors.iterator();
			while(neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				sum += next.f2.getValue();
			}
			if(next.f0 > 2) {
				out.collect(new Tuple2<Long, Long>(next.f0, sum));
				out.collect(new Tuple2<Long, Long>(next.f0, sum * 2));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo implements NeighborsFunction<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> next = null;
			Iterator<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighborsIterator =
					neighbors.iterator();
			while(neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				sum += next.f2.getValue() * next.f1.getValue();
			}
			if(next.f0 > 2) {
				out.collect(new Tuple2<Long, Long>(next.f0, sum));
				out.collect(new Tuple2<Long, Long>(next.f0, sum * 2));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumAllNeighborsNoValueMultipliedByTwoIdGreaterThanTwo implements NeighborsFunction<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Iterable<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>> next = null;
			Iterator<Tuple3<Long, Edge<Long, Long>, Vertex<Long, Long>>> neighborsIterator =
					neighbors.iterator();
			while(neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				sum += next.f2.getValue();
			}
			if(next.f0 > 2) {
				out.collect(new Tuple2<Long, Long>(next.f0, sum));
				out.collect(new Tuple2<Long, Long>(next.f0, sum * 2));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class SumOutNeighborsMultipliedByTwo implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum * 2));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumInNeighborsSubtractOne implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f0.getValue() * neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum));
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum - 1));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumAllNeighborsAddFive implements NeighborsFunctionWithVertexValue<Long, Long, Long,
			Tuple2<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, Long> vertex,
									 Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors,
									 Collector<Tuple2<Long, Long>> out) throws Exception {

			long sum = 0;
			for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
				sum += neighbor.f1.getValue();
			}
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum + vertex.getValue()));
			out.collect(new Tuple2<Long, Long>(vertex.getId(), sum + vertex.getValue() + 5));
		}
	}
}

