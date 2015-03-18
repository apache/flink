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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class DegreesWithExceptionITCase {

	private static final int PARALLELISM = 4;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void suppressOutput() {
		TestGraphUtils.pipeSystemOutToNull();
	}

	@BeforeClass
	public static void setupCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, PARALLELISM);
		cluster = new ForkableFlinkMiniCluster(config, false);
	}

	@AfterClass
	public static void tearDownCluster() {
		try {
			cluster.stop();
		}
		catch (Throwable t) {
			System.err.println("Error stopping cluster on shutdown");
			t.printStackTrace();
			fail("Cluster shutdown caused an exception: " + t.getMessage());
		}
	}

	/**
	 * Test outDegrees() with an edge having a srcId that does not exist in the vertex DataSet
	 */
	@Test
	public void testOutDegreesInvalidEdgeSrcId() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());

		env.setDegreeOfParallelism(PARALLELISM);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			graph.outDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.outDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			assertTrue("Root exception has to be of type ProgramInvocationException.", e instanceof ProgramInvocationException);
			assertTrue("ProgramInvocationException's cause has to be of type JobExecutionException.", e.getCause() instanceof JobExecutionException);
			assertTrue("JobExecutionException's cause has to be of type NoSuchElementException.", e.getCause().getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testInDegreesInvalidEdgeTrgId() throws Exception {
		/*
		* Test inDegrees() with an edge having a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());

		env.setDegreeOfParallelism(PARALLELISM);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			graph.inDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.inDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			assertTrue("Root exception has to be of type ProgramInvocationException.", e instanceof ProgramInvocationException);
			assertTrue("ProgramInvocationException's cause has to be of type JobExecutionException.", e.getCause() instanceof JobExecutionException);
			assertTrue("JobExecutionException's cause has to be of type NoSuchElementException.", e.getCause().getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeTrgId() throws Exception {
		/*
		* Test getDegrees() with an edge having a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());

		env.setDegreeOfParallelism(PARALLELISM);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			assertTrue("Root exception has to be of type ProgramInvocationException.", e instanceof ProgramInvocationException);
			assertTrue("ProgramInvocationException's cause has to be of type JobExecutionException.", e.getCause() instanceof JobExecutionException);
			assertTrue("JobExecutionException's cause has to be of type NoSuchElementException.", e.getCause().getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeSrcId() throws Exception {
		/*
		* Test getDegrees() with an edge having a srcId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());

		env.setDegreeOfParallelism(PARALLELISM);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			assertTrue("Root exception has to be of type ProgramInvocationException.", e instanceof ProgramInvocationException);
			assertTrue("ProgramInvocationException's cause has to be of type JobExecutionException.", e.getCause() instanceof JobExecutionException);
			assertTrue("JobExecutionException's cause has to be of type NoSuchElementException.", e.getCause().getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeSrcTrgId() throws Exception {
		/*
		* Test getDegrees() with an edge having a srcId and a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getJobManagerRPCPort());

		env.setDegreeOfParallelism(PARALLELISM);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcTrgData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			assertTrue("Root exception has to be of type ProgramInvocationException.", e instanceof ProgramInvocationException);
			assertTrue("ProgramInvocationException's cause has to be of type JobExecutionException.", e.getCause() instanceof JobExecutionException);
			assertTrue("JobExecutionException's cause has to be of type NoSuchElementException.", e.getCause().getCause() instanceof NoSuchElementException);
		}
	}
}
