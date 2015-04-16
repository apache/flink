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

import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.IterationConfiguration;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VertexCentricConfigurationITCase extends MultipleProgramsTestBase {

	public VertexCentricConfigurationITCase(TestExecutionMode mode){
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
	public void testRunWithConfiguration() throws Exception {
		/*
		 * Test Graph's runVertexCentricIteration when configuration parameters are provided
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(), 
				TestGraphUtils.getLongLongEdges(), env).mapVertices(new AssignOneMapper());

		// create the configuration object
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();

		parameters.addBroadcastSetForUpdateFunction("updateBcastSet", env.fromElements(1, 2, 3));
		parameters.addBroadcastSetForMessagingFunction("messagingBcastSet", env.fromElements(4, 5, 6));
		parameters.registerAggregator("superstepAggregator", new LongSumAggregator());

		Graph<Long, Long, Long> result = graph.runVertexCentricIteration(
				new UpdateFunction(), new MessageFunction(), 10, parameters);

		result.getVertices().writeAsCsv(resultPath, "\n", "\t");
		env.execute();
		expectedResult = "1	11\n" +
						"2	11\n" +
						"3	11\n" +
						"4	11\n" +
						"5	11";
	}

	@Test
	public void testIterationConfiguration() throws Exception {

		/*
		 * Test name, parallelism and solutionSetUnmanaged parameters
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		VertexCentricIteration<Long, Long, Long, Long> iteration = VertexCentricIteration
				.withEdges(TestGraphUtils.getLongLongEdgeData(env), new DummyUpdateFunction(), 
						new DummyMessageFunction(), 10);
		
		VertexCentricConfiguration parameters = new VertexCentricConfiguration();
		parameters.setName("gelly iteration");
		parameters.setParallelism(2);
		parameters.setSolutionSetUnmanagedMemory(true);
		
		iteration.configure(parameters);
		
		Assert.assertEquals("gelly iteration", iteration.getIterationConfiguration().getName(""));
		Assert.assertEquals(2, iteration.getIterationConfiguration().getParallelism());
		Assert.assertEquals(true, iteration.getIterationConfiguration().isSolutionSetUnmanagedMemory());

		DataSet<Vertex<Long, Long>> result = TestGraphUtils.getLongLongVertexData(env).runOperation(iteration);
		
		result.writeAsCsv(resultPath, "\n", "\t");
		env.execute();
		expectedResult = "1	11\n" +
						"2	12\n" +
						"3	13\n" +
						"4	14\n" +
						"5	15";
	}

	@Test
	public void testIterationDefaultDirection() throws Exception {

		/*
		 * Test that if no direction parameter is given, the iteration works as before
		 * (i.e. it collects messages from the in-neighbors and sends them to the out-neighbors)
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, HashSet<Long>, Long> graph = Graph
				.fromCollection(TestGraphUtils.getLongLongVertices(), TestGraphUtils.getLongLongEdges(), env)
				.mapVertices(new InitialiseHashSetMapper());

		DataSet<Vertex<Long, HashSet<Long>>> resultedVertices = graph
				.runVertexCentricIteration(new VertexUpdateDirection(), new IdMessengerTrg(), 5)
				.getVertices();

		resultedVertices.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	[5]\n" +
				"2	[1]\n" +
				"3	[1, 2]\n" +
				"4	[3]\n" +
				"5	[3, 4]";
	}

	@Test
	public void testIterationINDirection() throws Exception {

		/*
		 * Test that if the direction parameter is set to IN,
		 * messages are collected from the out-neighbors and sent to the in-neighbors.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, HashSet<Long>, Long> graph = Graph
				.fromCollection(TestGraphUtils.getLongLongVertices(), TestGraphUtils.getLongLongEdges(), env)
				.mapVertices(new InitialiseHashSetMapper());

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setDirection(EdgeDirection.IN);

		DataSet<Vertex<Long, HashSet<Long>>> resultedVertices = graph
				.runVertexCentricIteration(new VertexUpdateDirection(), new IdMessengerSrc(), 5, parameters)
				.getVertices();

		resultedVertices.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	[2, 3]\n" +
				"2	[3]\n" +
				"3	[4, 5]\n" +
				"4	[5]\n" +
				"5	[1]";
	}

	@Test
	public void testIterationALLDirection() throws Exception {

		/*
		 * Test that if the direction parameter is set to ALL,
		 * messages are collected from all the neighbors and sent to all the neighbors.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, HashSet<Long>, Long> graph = Graph
				.fromCollection(TestGraphUtils.getLongLongVertices(), TestGraphUtils.getLongLongEdges(), env)
				.mapVertices(new InitialiseHashSetMapper());

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setDirection(EdgeDirection.ALL);

		DataSet<Vertex<Long, HashSet<Long>>> resultedVertices = graph
				.runVertexCentricIteration(new VertexUpdateDirection(), new IdMessengerAll(), 5, parameters)
				.getVertices();

		resultedVertices.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	[2, 3, 5]\n" +
				"2	[1, 3]\n" +
				"3	[1, 2, 4, 5]\n" +
				"4	[3, 5]\n" +
				"5	[1, 3, 4]";
	}

	@Test
	public void testInDegreesSet() throws Exception {

		/*
		 * Test that if the degrees are set, the in degrees can be accessed in every superstep and the value
		 * is correctly computed.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setOptDegrees(true);

		DataSet<Vertex<Long, Long>> verticesWithInDegree = graph.runVertexCentricIteration(new UpdateFunctionInDegree(),
				new DummyMessageFunction(), 5, parameters).getVertices();

		verticesWithInDegree.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	1\n" +
				"2	1\n" +
				"3	2\n" +
				"4	1\n" +
				"5	2";
	}

	@Test
	public void testOutDegreesSet() throws Exception {

		/*
		 * Test that if the degrees are set, the out degrees can be accessed in every superstep and the value
		 * is correctly computed.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setOptDegrees(true);

		DataSet<Vertex<Long, Long>> verticesWithOutDegree = graph.runVertexCentricIteration(new UpdateFunctionOutDegree(),
				new DummyMessageFunction(), 5, parameters).getVertices();

		verticesWithOutDegree.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	2\n" +
				"2	1\n" +
				"3	2\n" +
				"4	1\n" +
				"5	1";
	}

	@Test
	public void testNumVerticesSet() throws Exception {

		/*
		 * Test that if the number of vertices option is set, it can be accessed in every superstep and the value
		 * is correctly computed.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setOptNumVertices(true);

		DataSet<Vertex<Long, Long>> verticesWithNumVertices = graph.runVertexCentricIteration(new UpdateFunctionNumVertices(),
				new DummyMessageFunction(), 5, parameters).getVertices();

		verticesWithNumVertices.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	5\n" +
				"2	5\n" +
				"3	5\n" +
				"4	5\n" +
				"5	5";
	}

	@Test
	public void testDegrees() throws Exception {

		/*
		 * Test that if the degrees are set, they can be accessed in every superstep and the value
		 * is correctly computed for both in and out degrees.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Tuple3<Long, Long, Boolean>, Long> graph = Graph.fromCollection(TestGraphUtils.getLongVerticesWithDegrees(),
				TestGraphUtils.getLongLongEdges(), env);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setOptDegrees(true);

		DataSet<Vertex<Long, Tuple3<Long, Long, Boolean>>> verticesWithDegrees = graph.runVertexCentricIteration(
				new UpdateFunctionDegrees(), new DegreeMessageFunction(), 5, parameters).getVertices();

		verticesWithDegrees.map(new MapFunction<Vertex<Long,Tuple3<Long,Long,Boolean>>, Tuple2<Long, Boolean>>() {
			@Override
			public Tuple2<Long, Boolean> map(Vertex<Long, Tuple3<Long, Long, Boolean>> vertex) throws Exception {
				return new Tuple2<Long, Boolean>(vertex.getId(), vertex.getValue().f2);
			}
		}).writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	true\n" +
				"2	true\n" +
				"3	true\n" +
				"4	true\n" +
				"5	true";
	}

	@Test
	public void testDirectionALLAndDegrees() throws Exception {

		/*
		 * Compute the number of neighbors in a vertex - centric manner, and verify that it is equal to
		 * the sum: inDegree + outDegree.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Boolean, Long> graph = Graph.fromCollection(TestGraphUtils.getLongBooleanVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		// configure the iteration
		IterationConfiguration parameters = new IterationConfiguration();

		parameters.setOptDegrees(true);
		parameters.setDirection(EdgeDirection.ALL);

		DataSet<Vertex<Long, Boolean>> verticesWithNumNeighbors = graph.runVertexCentricIteration(
				new VertexUpdateNumNeighbors(), new IdMessenger(), 1, parameters).getVertices();

		verticesWithNumNeighbors.writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	true\n" +
				"2	true\n" +
				"3	true\n" +
				"4	true\n" +
				"5	true";
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunction extends VertexUpdateFunction<Long, Long, Long> {

		LongSumAggregator aggregator = new LongSumAggregator();

		@Override
		public void preSuperstep() {
			
			// test bcast variable
			@SuppressWarnings("unchecked")
			List<Tuple1<Integer>> bcastSet = (List<Tuple1<Integer>>)(List<?>)getBroadcastSet("updateBcastSet");
			Assert.assertEquals(1, bcastSet.get(0));
			Assert.assertEquals(2, bcastSet.get(1));
			Assert.assertEquals(3, bcastSet.get(2));
			
			// test aggregator
			aggregator = getIterationAggregator("superstepAggregator");
		}

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			long superstep = getSuperstepNumber();
			aggregator.aggregate(superstep);
			setNewVertexValue(vertex.getValue() + 1);
		}
	}
	
	@SuppressWarnings("serial")
	public static final class MessageFunction extends MessagingFunction<Long, Long, Long, Long> {

		@Override
		public void preSuperstep() {
			
			// test bcast variable
			@SuppressWarnings("unchecked")
			List<Tuple1<Integer>> bcastSet = (List<Tuple1<Integer>>)(List<?>)getBroadcastSet("messagingBcastSet");
			Assert.assertEquals(4, bcastSet.get(0));
			Assert.assertEquals(5, bcastSet.get(1));
			Assert.assertEquals(6, bcastSet.get(2));
			
			// test aggregator
			if (getSuperstepNumber() == 2) {
				long aggrValue = ((LongValue)getPreviousIterationAggregate("superstepAggregator")).getValue();
				Assert.assertEquals(5, aggrValue);
			}
		}

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			//send message to keep vertices active
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionInDegree extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			try {
				setNewVertexValue(vertex.getInDegree());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionOutDegree extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			try {
				setNewVertexValue(vertex.getOutDegree());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionNumVertices extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			try {
				setNewVertexValue(getNumberOfVertices());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class DummyUpdateFunction extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			setNewVertexValue(vertex.getValue() + 1);
		}
	}
	
	@SuppressWarnings("serial")
	public static final class DummyMessageFunction extends MessagingFunction<Long, Long, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			//send message to keep vertices active
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

	@SuppressWarnings("serial")
	public static final class VertexUpdateDirection extends VertexUpdateFunction<Long, HashSet<Long>,
			Long> {

		@Override
		public void updateVertex(Vertex<Long, HashSet<Long>> vertex, MessageIterator<Long> messages) throws Exception {
			vertex.getValue().clear();

			for(long msg : messages) {
				vertex.getValue().add(msg);
			}

			setNewVertexValue(vertex.getValue());
		}
	}

	@SuppressWarnings("serial")
	public static final class VertexUpdateNumNeighbors extends VertexUpdateFunction<Long, Boolean,
			Long> {

		@Override
		public void updateVertex(Vertex<Long, Boolean> vertex, MessageIterator<Long> messages) throws Exception {

			long count = 0;

			for(long msg : messages) {
				count++;
			}

			setNewVertexValue(count == (vertex.getInDegree() + vertex.getOutDegree()));
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionDegrees extends VertexUpdateFunction<Long, Tuple3<Long, Long, Boolean>, Long> {

		@Override
		public void updateVertex(Vertex<Long, Tuple3<Long, Long, Boolean>> vertex, MessageIterator<Long> inMessages) {
			try {
				setNewVertexValue(new Tuple3(vertex.getValue().f0, vertex.getValue().f1, (vertex.getInDegree() == vertex.getValue().f0)
						&& (vertex.getOutDegree() == vertex.getValue().f1) && vertex.getValue().f2));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdMessengerSrc extends MessagingFunction<Long, HashSet<Long>, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
			for (Edge<Long, Long> edge : getEdges()) {
				sendMessageTo(edge.getSource(), vertex.getId());
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdMessengerAll extends MessagingFunction<Long, HashSet<Long>, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
			for (Edge<Long, Long> edge : getEdges()) {
				if(edge.getSource() != vertex.getId()) {
					sendMessageTo(edge.getSource(), vertex.getId());
				} else {
					sendMessageTo(edge.getTarget(), vertex.getId());
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdMessenger extends MessagingFunction<Long, Boolean, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, Boolean> vertex) throws Exception {
			for (Edge<Long, Long> edge : getEdges()) {
				if(edge.getSource() != vertex.getId()) {
					sendMessageTo(edge.getSource(), vertex.getId());
				} else {
					sendMessageTo(edge.getTarget(), vertex.getId());
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdMessengerTrg extends MessagingFunction<Long, HashSet<Long>, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
			for (Edge<Long, Long> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getId());
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class DegreeMessageFunction extends MessagingFunction<Long, Tuple3<Long, Long, Boolean>, Long, Long> {

		@Override
		public void sendMessages(Vertex<Long, Tuple3<Long, Long, Boolean>> vertex) {
			//send message to keep vertices active
			sendMessageToAllNeighbors(vertex.getValue().f0);
		}
	}

	@SuppressWarnings("serial")
	public static final class AssignOneMapper implements MapFunction<Vertex<Long, Long>, Long> {

		public Long map(Vertex<Long, Long> value) {
			return 1l;
		}
	}

	@SuppressWarnings("serial")
	public static final class InitialiseHashSetMapper implements MapFunction<Vertex<Long, Long>, HashSet<Long>> {

		@Override
		public HashSet<Long> map(Vertex<Long, Long> value) throws Exception {
			return new HashSet<Long>();
		}
	}
}
