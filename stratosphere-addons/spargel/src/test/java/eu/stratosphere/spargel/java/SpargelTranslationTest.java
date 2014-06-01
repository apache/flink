/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.spargel.java;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.DeltaIterationResultSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.TwoInputUdfOperator;
import eu.stratosphere.api.java.tuple.Tuple2;


@SuppressWarnings("serial")
public class SpargelTranslationTest {

	@Test
	public void testTranslationPlainEdges() {
		try {
			final String ITERATION_NAME = "Test Name";
			
			final String AGGREGATOR_NAME = "AggregatorName";
			
			final String BC_SET_MESSAGES_NAME = "borat messages";
			
			final String BC_SET_UPDATES_NAME = "borat updates";
			;
			final int NUM_ITERATIONS = 13;
			
			final int ITERATION_DOP = 77;
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> bcMessaging = env.fromElements(1L);
			DataSet<Long> bcUpdate = env.fromElements(1L);
			
			DataSet<Tuple2<String, Double>> result;
			
			// ------------ construct the test program ------------------
			{
				
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<String, Double>> initialVertices = env.fromElements(new Tuple2<String, Double>("abc", 3.44));
	
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<String, String>> edges = env.fromElements(new Tuple2<String, String>("a", "c"));
				
				
				VertexCentricIteration<String, Double, Long, ?> vertexIteration = 
						VertexCentricIteration.withPlainEdges(edges, new UpdateFunction(), new MessageFunctionNoEdgeValue(), NUM_ITERATIONS);
				vertexIteration.addBroadcastSetForMessagingFunction(BC_SET_MESSAGES_NAME, bcMessaging);
				vertexIteration.addBroadcastSetForUpdateFunction(BC_SET_UPDATES_NAME, bcUpdate);
				
				vertexIteration.setName(ITERATION_NAME);
				vertexIteration.setParallelism(ITERATION_DOP);
				
				vertexIteration.registerAggregator(AGGREGATOR_NAME, LongSumAggregator.class);
				
				result = initialVertices.runOperation(vertexIteration);
			}
			
			
			// ------------- validate the java program ----------------
			
			assertTrue(result instanceof DeltaIterationResultSet);
			
			DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
			DeltaIteration<?, ?> iteration = (DeltaIteration<?, ?>) resultSet.getIterationHead();
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, resultSet.getMaxIterations());
			assertArrayEquals(new int[] {0}, resultSet.getKeyPositions());
			assertEquals(ITERATION_DOP, iteration.getParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			assertEquals(AGGREGATOR_NAME, iteration.getAggregators().getAllRegisteredAggregators().iterator().next().getName());
			
			// validate that the semantic properties are set as they should
			TwoInputUdfOperator<?, ?, ?, ?> solutionSetJoin = (TwoInputUdfOperator<?, ?, ?, ?>) resultSet.getNextWorkset();
			assertTrue(solutionSetJoin.getSematicProperties().getForwardedField1(0).contains(0));
			assertTrue(solutionSetJoin.getSematicProperties().getForwardedField2(0).contains(0));
			
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
			;
			final int NUM_ITERATIONS = 13;
			
			final int ITERATION_DOP = 77;
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> bcVar = env.fromElements(1L);
			
			DataSet<Tuple2<String, Double>> result;
			
			// ------------ construct the test program ------------------
			{
				
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<String, Double>> initialVertices = env.fromElements(new Tuple2<String, Double>("abc", 3.44));
	
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<String, String>> edges = env.fromElements(new Tuple2<String, String>("a", "c"));
				
				
				VertexCentricIteration<String, Double, Long, ?> vertexIteration = 
						VertexCentricIteration.withPlainEdges(edges, new UpdateFunction(), new MessageFunctionNoEdgeValue(), NUM_ITERATIONS);
				vertexIteration.addBroadcastSetForMessagingFunction(BC_SET_MESSAGES_NAME, bcVar);
				vertexIteration.addBroadcastSetForUpdateFunction(BC_SET_UPDATES_NAME, bcVar);
				
				vertexIteration.setName(ITERATION_NAME);
				vertexIteration.setParallelism(ITERATION_DOP);
				
				vertexIteration.registerAggregator(AGGREGATOR_NAME, LongSumAggregator.class);
				
				result = initialVertices.runOperation(vertexIteration);
			}
			
			
			// ------------- validate the java program ----------------
			
			assertTrue(result instanceof DeltaIterationResultSet);
			
			DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
			DeltaIteration<?, ?> iteration = (DeltaIteration<?, ?>) resultSet.getIterationHead();
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, resultSet.getMaxIterations());
			assertArrayEquals(new int[] {0}, resultSet.getKeyPositions());
			assertEquals(ITERATION_DOP, iteration.getParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			assertEquals(AGGREGATOR_NAME, iteration.getAggregators().getAllRegisteredAggregators().iterator().next().getName());
			
			// validate that the semantic properties are set as they should
			TwoInputUdfOperator<?, ?, ?, ?> solutionSetJoin = (TwoInputUdfOperator<?, ?, ?, ?>) resultSet.getNextWorkset();
			assertTrue(solutionSetJoin.getSematicProperties().getForwardedField1(0).contains(0));
			assertTrue(solutionSetJoin.getSematicProperties().getForwardedField2(0).contains(0));
			
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
		public void updateVertex(String vertexKey, Double vertexValue, MessageIterator<Long> inMessages) {}
	}
	
	public static class MessageFunctionNoEdgeValue extends MessagingFunction<String, Double, Long, Object> {

		@Override
		public void sendMessages(String vertexKey, Double vertexValue) {}
	}
}
