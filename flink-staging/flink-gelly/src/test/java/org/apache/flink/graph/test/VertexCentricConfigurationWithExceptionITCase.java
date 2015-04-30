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
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexWithDegrees;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class VertexCentricConfigurationWithExceptionITCase {

	private static final int PARALLELISM = 4;

	private static ForkableFlinkMiniCluster cluster;


	@BeforeClass
	public static void setupCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, PARALLELISM);
			cluster = new ForkableFlinkMiniCluster(config, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error starting test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void tearDownCluster() {
		try {
			cluster.stop();
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Cluster shutdown caused an exception: " + t.getMessage());
		}
	}

	@Test
	public void testOutDegreesNotSet() throws Exception {

		/*
		 * Test that if the degrees are not set, the out degrees cannot be accessed - an
		 * exception is thrown.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		try {
			DataSet<Vertex<Long, Long>> verticesWithOutDegrees = graph.runVertexCentricIteration(new UpdateFunctionOutDegree(),
					new DummyMessageFunction(), 5).getVertices();

			verticesWithOutDegrees.output(new DiscardingOutputFormat<Vertex<Long, Long>>());
			env.execute();

			fail("The degree option not set test did not fail");
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	@Test
	public void testInDegreesNotSet() throws Exception {

		/*
		 * Test that if the degrees are not set, the in degrees cannot be accessed - an
		 * exception is thrown.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		try {
			DataSet<Vertex<Long, Long>> verticesWithInDegrees = graph.runVertexCentricIteration(new UpdateFunctionInDegree(),
					new DummyMessageFunction(), 5).getVertices();

			verticesWithInDegrees.output(new DiscardingOutputFormat<Vertex<Long, Long>>());
			env.execute();

			fail("The degree option not set test did not fail");
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	@Test
	public void testNumVerticesNotSet() throws Exception {

		/*
		 * Test that if the number of vertices option is not set, this number cannot be accessed -
		 * an exception id thrown.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env);

		try {
			DataSet<Vertex<Long, Long>> verticesWithNumVertices = graph.runVertexCentricIteration(new UpdateFunctionNumVertices(),
					new DummyMessageFunction(), 5).getVertices();

			verticesWithNumVertices.output(new DiscardingOutputFormat<Vertex<Long, Long>>());
			env.execute();

			fail("The num vertices option not set test did not fail");
		} catch (Exception e) {
			// We expect the job to fail with an exception
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionInDegree extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) throws Exception {
			setNewVertexValue(((VertexWithDegrees) vertex).getInDegree());
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionOutDegree extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) throws Exception {
			setNewVertexValue(((VertexWithDegrees)vertex).getOutDegree());
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFunctionNumVertices extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) throws Exception {
			setNewVertexValue(getNumberOfVertices());
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
}
