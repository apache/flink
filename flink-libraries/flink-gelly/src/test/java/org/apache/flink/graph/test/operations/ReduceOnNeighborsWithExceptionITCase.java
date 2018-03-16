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
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test expected exceptions for {@link Graph#groupReduceOnNeighbors} and {@link Graph#reduceOnNeighbors}.
 */
public class ReduceOnNeighborsWithExceptionITCase extends AbstractTestBase {

	private static final int PARALLELISM = 4;

	/**
	 * Test groupReduceOnNeighbors() -NeighborsFunctionWithVertexValue-
	 * with an edge having a srcId that does not exist in the vertex DataSet.
	 */
	@Test
	public void testGroupReduceOnNeighborsWithVVInvalidEdgeSrcId() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
					graph.groupReduceOnNeighbors(new SumAllNeighbors(), EdgeDirection.ALL);

			verticesWithSumOfOutNeighborValues.output(new DiscardingOutputFormat<>());
			env.execute();

			fail("Expected an exception.");
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	/**
	 * Test groupReduceOnNeighbors() -NeighborsFunctionWithVertexValue-
	 * with an edge having a trgId that does not exist in the vertex DataSet.
	 */
	@Test
	public void testGroupReduceOnNeighborsWithVVInvalidEdgeTrgId() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues =
					graph.groupReduceOnNeighbors(new SumAllNeighbors(), EdgeDirection.ALL);

			verticesWithSumOfOutNeighborValues.output(new DiscardingOutputFormat<>());
			env.execute();

			fail("Expected an exception.");
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	/**
	 * Test groupReduceOnNeighbors() -NeighborsFunction-
	 * with an edge having a srcId that does not exist in the vertex DataSet.
	 */
	@Test
	public void testGroupReduceOnNeighborsInvalidEdgeSrcId() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			DataSet<Tuple2<Long, Long>> verticesWithSumOfAllNeighborValues =
					graph.reduceOnNeighbors(new SumNeighbors(), EdgeDirection.ALL);

			verticesWithSumOfAllNeighborValues.output(new DiscardingOutputFormat<>());
			env.execute();
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	/**
	 * Test groupReduceOnNeighbors() -NeighborsFunction-
	 * with an edge having a trgId that does not exist in the vertex DataSet.
	 */
	@Test
	public void testGroupReduceOnNeighborsInvalidEdgeTrgId() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			DataSet<Tuple2<Long, Long>> verticesWithSumOfAllNeighborValues =
					graph.reduceOnNeighbors(new SumNeighbors(), EdgeDirection.ALL);

			verticesWithSumOfAllNeighborValues.output(new DiscardingOutputFormat<>());
			env.execute();
		} catch (Exception e) {
			// We expect the job to fail with an exception
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
			out.collect(new Tuple2<>(vertex.getId(), sum + vertex.getValue()));
		}
	}

	@SuppressWarnings("serial")
	private static final class SumNeighbors implements ReduceNeighborsFunction<Long> {

		@Override
		public Long reduceNeighbors(Long firstNeighbor, Long secondNeighbor) {
			return firstNeighbor + secondNeighbor;
		}
	}
}
