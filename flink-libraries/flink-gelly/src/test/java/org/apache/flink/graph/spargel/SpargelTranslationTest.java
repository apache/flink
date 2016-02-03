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


package org.apache.flink.graph.spargel;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.DeltaIterationResultSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

@SuppressWarnings("serial")
public class SpargelTranslationTest {

	@Test
	public void testTranslationPlainEdges() {
		try {
			final String ITERATION_NAME = "Test Name";
			
			final String AGGREGATOR_NAME = "AggregatorName";
			
			final String BC_SET_MESSAGES_NAME = "borat messages";
			
			final String BC_SET_UPDATES_NAME = "borat updates";

			final int NUM_ITERATIONS = 13;
			
			final int ITERATION_parallelism = 77;
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> bcMessaging = env.fromElements(1L);
			DataSet<Long> bcUpdate = env.fromElements(1L);
			
			DataSet<Vertex<String, Double>> result;
			
			// ------------ construct the test program ------------------
			{
				
				DataSet<Tuple2<String, Double>> initialVertices = env.fromElements(new Tuple2<>("abc", 3.44));

				DataSet<Tuple2<String, String>> edges = env.fromElements(new Tuple2<>("a", "c"));

				Graph<String, Double, NullValue> graph = Graph.fromTupleDataSet(initialVertices,
						edges.map(new MapFunction<Tuple2<String,String>, Tuple3<String, String, NullValue>>() {

							public Tuple3<String, String, NullValue> map(
									Tuple2<String, String> edge) {
								return new Tuple3<>(edge.f0, edge.f1, NullValue.getInstance());
							}
						}), env);

				ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

				parameters.addBroadcastSetForMessagingFunction(BC_SET_MESSAGES_NAME, bcMessaging);
				parameters.addBroadcastSetForUpdateFunction(BC_SET_UPDATES_NAME, bcUpdate);
				parameters.setName(ITERATION_NAME);
				parameters.setParallelism(ITERATION_parallelism);
				parameters.registerAggregator(AGGREGATOR_NAME, new LongSumAggregator());

				result = graph.runScatterGatherIteration(new UpdateFunction(), new MessageFunctionNoEdgeValue(),
						NUM_ITERATIONS, parameters).getVertices();

				result.output(new DiscardingOutputFormat<Vertex<String, Double>>());
			}
			
			
			// ------------- validate the java program ----------------
			
			assertTrue(result instanceof DeltaIterationResultSet);
			
			DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
			DeltaIteration<?, ?> iteration = resultSet.getIterationHead();
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, resultSet.getMaxIterations());
			assertArrayEquals(new int[] {0}, resultSet.getKeyPositions());
			assertEquals(ITERATION_parallelism, iteration.getParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			assertEquals(AGGREGATOR_NAME, iteration.getAggregators().getAllRegisteredAggregators().iterator().next().getName());
			
			// validate that the semantic properties are set as they should
			TwoInputUdfOperator<?, ?, ?, ?> solutionSetJoin = (TwoInputUdfOperator<?, ?, ?, ?>) resultSet.getNextWorkset();
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(0, 0).contains(0));
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(1, 0).contains(0));
			
			TwoInputUdfOperator<?, ?, ?, ?> edgesJoin = (TwoInputUdfOperator<?, ?, ?, ?>) solutionSetJoin.getInput1();
			
			// validate that the broadcast sets are forwarded
			assertEquals(bcUpdate, solutionSetJoin.getBroadcastSets().get(BC_SET_UPDATES_NAME));
			assertEquals(bcMessaging, edgesJoin.getBroadcastSets().get(BC_SET_MESSAGES_NAME));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTranslationPlainEdgesWithForkedBroadcastVariable() {
		try {
			final String ITERATION_NAME = "Test Name";
			
			final String AGGREGATOR_NAME = "AggregatorName";
			
			final String BC_SET_MESSAGES_NAME = "borat messages";
			
			final String BC_SET_UPDATES_NAME = "borat updates";

			final int NUM_ITERATIONS = 13;
			
			final int ITERATION_parallelism = 77;
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> bcVar = env.fromElements(1L);
			
			DataSet<Vertex<String, Double>> result;
			
			// ------------ construct the test program ------------------
			{

				DataSet<Tuple2<String, Double>> initialVertices = env.fromElements(new Tuple2<>("abc", 3.44));

				DataSet<Tuple2<String, String>> edges = env.fromElements(new Tuple2<>("a", "c"));

				Graph<String, Double, NullValue> graph = Graph.fromTupleDataSet(initialVertices,
						edges.map(new MapFunction<Tuple2<String,String>, Tuple3<String, String, NullValue>>() {

							public Tuple3<String, String, NullValue> map(
									Tuple2<String, String> edge) {
								return new Tuple3<>(edge.f0, edge.f1, NullValue.getInstance());
							}
						}), env);

				ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

				parameters.addBroadcastSetForMessagingFunction(BC_SET_MESSAGES_NAME, bcVar);
				parameters.addBroadcastSetForUpdateFunction(BC_SET_UPDATES_NAME, bcVar);
				parameters.setName(ITERATION_NAME);
				parameters.setParallelism(ITERATION_parallelism);
				parameters.registerAggregator(AGGREGATOR_NAME, new LongSumAggregator());
				
				result = graph.runScatterGatherIteration(new UpdateFunction(), new MessageFunctionNoEdgeValue(),
						NUM_ITERATIONS, parameters).getVertices();

				result.output(new DiscardingOutputFormat<Vertex<String, Double>>());
			}
			
			
			// ------------- validate the java program ----------------
			
			assertTrue(result instanceof DeltaIterationResultSet);
			
			DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
			DeltaIteration<?, ?> iteration = resultSet.getIterationHead();
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, resultSet.getMaxIterations());
			assertArrayEquals(new int[] {0}, resultSet.getKeyPositions());
			assertEquals(ITERATION_parallelism, iteration.getParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			assertEquals(AGGREGATOR_NAME, iteration.getAggregators().getAllRegisteredAggregators().iterator().next().getName());
			
			// validate that the semantic properties are set as they should
			TwoInputUdfOperator<?, ?, ?, ?> solutionSetJoin = (TwoInputUdfOperator<?, ?, ?, ?>) resultSet.getNextWorkset();
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(0, 0).contains(0));
			assertTrue(solutionSetJoin.getSemanticProperties().getForwardingTargetFields(1, 0).contains(0));
			
			TwoInputUdfOperator<?, ?, ?, ?> edgesJoin = (TwoInputUdfOperator<?, ?, ?, ?>) solutionSetJoin.getInput1();
			
			// validate that the broadcast sets are forwarded
			assertEquals(bcVar, solutionSetJoin.getBroadcastSets().get(BC_SET_UPDATES_NAME));
			assertEquals(bcVar, edgesJoin.getBroadcastSets().get(BC_SET_MESSAGES_NAME));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class UpdateFunction extends VertexUpdateFunction<String, Double, Long> {

		@Override
		public void updateVertex(Vertex<String, Double> vertex, MessageIterator<Long> inMessages) {}
	}
	
	public static class MessageFunctionNoEdgeValue extends MessagingFunction<String, Double, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<String, Double> vertex) {}
	}
}
