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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class SimpleRecoveryITCase {

	private static NepheleMiniCluster cluster;

	@BeforeClass
	public static void setupCluster() {
		try {
			cluster = new NepheleMiniCluster();
			cluster.setNumTaskManager(2);
			cluster.setTaskManagerNumSlots(2);

			// these two parameters determine how fast the restart happens
			cluster.setHeartbeatInterval(500);
			cluster.setHeartbeatTimeout(2000);

			cluster.start();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Could not start test minicluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void tearDownCluster() {
		try {
			cluster.stop();
		}
		catch (Exception e) {
			System.err.println("Error stopping cluster on shutdown");
			e.printStackTrace();
			fail("Cluster shutdown caused an exception: " + e.getMessage());
		}
	}

	@Test
	public void testFailedRunThenSuccessfulRun() {

		try {
			List<Long> resultCollection = new ArrayList<Long>();

			// attempt 1
			{
				ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
						"localhost", cluster.getJobManagerRpcPort());

				env.setDegreeOfParallelism(4);
				env.setNumberOfExecutionRetries(0);

				env.generateSequence(1, 10)
						.rebalance()
						.map(new FailingMapper1<Long>())
						.reduce(new ReduceFunction<Long>() {
							@Override
							public Long reduce(Long value1, Long value2) {
								return value1 + value2;
							}
						})
						.output(new LocalCollectionOutputFormat<Long>(resultCollection));

				try {
					JobExecutionResult res = env.execute();
					String msg = res == null ? "null result" : "result in " + res.getNetRuntime();
					fail("The program should have failed, but returned " + msg);
				}
				catch (ProgramInvocationException e) {
					// expected
				}
			}

			// attempt 2
			{
				ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
						"localhost", cluster.getJobManagerRpcPort());

				env.setDegreeOfParallelism(4);
				env.setNumberOfExecutionRetries(0);

				env.generateSequence(1, 10)
						.rebalance()
						.map(new FailingMapper1<Long>())
						.reduce(new ReduceFunction<Long>() {
							@Override
							public Long reduce(Long value1, Long value2) {
								return value1 + value2;
							}
						})
						.output(new LocalCollectionOutputFormat<Long>(resultCollection));

				try {
					JobExecutionResult result = env.execute();
					assertTrue(result.getNetRuntime() >= 0);
					assertNotNull(result.getAllAccumulatorResults());
					assertTrue(result.getAllAccumulatorResults().isEmpty());
				}
				catch (JobExecutionException e) {
					fail("The program should have succeeded on the second run");
				}

				long sum = 0;
				for (long l : resultCollection) {
					sum += l;
				}
				assertEquals(55, sum);
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRestart() {
		try {
			List<Long> resultCollection = new ArrayList<Long>();

			ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getJobManagerRpcPort());

			env.setDegreeOfParallelism(4);
			env.setNumberOfExecutionRetries(1);

			env.generateSequence(1, 10)
					.rebalance()
					.map(new FailingMapper2<Long>())
					.reduce(new ReduceFunction<Long>() {
						@Override
						public Long reduce(Long value1, Long value2) {
							return value1 + value2;
						}
					})
					.output(new LocalCollectionOutputFormat<Long>(resultCollection));

			try {
				JobExecutionResult result = env.execute();
				assertTrue(result.getNetRuntime() >= 0);
				assertNotNull(result.getAllAccumulatorResults());
				assertTrue(result.getAllAccumulatorResults().isEmpty());
			}
			catch (JobExecutionException e) {
				fail("The program should have succeeded on the second run");
			}

			long sum = 0;
			for (long l : resultCollection) {
				sum += l;
			}
			assertEquals(55, sum);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRestartMultipleTimes() {
		try {
			List<Long> resultCollection = new ArrayList<Long>();

			ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getJobManagerRpcPort());

			env.setDegreeOfParallelism(4);
			env.setNumberOfExecutionRetries(3);

			env.generateSequence(1, 10)
					.rebalance()
					.map(new FailingMapper3<Long>())
					.reduce(new ReduceFunction<Long>() {
						@Override
						public Long reduce(Long value1, Long value2) {
							return value1 + value2;
						}
					})
					.output(new LocalCollectionOutputFormat<Long>(resultCollection));

			try {
				JobExecutionResult result = env.execute();
				assertTrue(result.getNetRuntime() >= 0);
				assertNotNull(result.getAllAccumulatorResults());
				assertTrue(result.getAllAccumulatorResults().isEmpty());
			}
			catch (JobExecutionException e) {
				fail("The program should have succeeded on the second run");
			}

			long sum = 0;
			for (long l : resultCollection) {
				sum += l;
			}
			assertEquals(55, sum);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------------------

	private static class FailingMapper1<T> extends RichMapFunction<T, T> {

		private static int failuresBeforeSuccess = 1;

		@Override
		public T map(T value) throws Exception {
			if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
				failuresBeforeSuccess--;
				throw new Exception("Test Failure");
			}

			return value;
		}
	}

	private static class FailingMapper2<T> extends RichMapFunction<T, T> {

		private static int failuresBeforeSuccess = 1;

		@Override
		public T map(T value) throws Exception {
			if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
				failuresBeforeSuccess--;
				throw new Exception("Test Failure");
			}

			return value;
		}
	}

	private static class FailingMapper3<T> extends RichMapFunction<T, T> {

		private static int failuresBeforeSuccess = 3;

		@Override
		public T map(T value) throws Exception {
			if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
				failuresBeforeSuccess--;
				throw new Exception("Test Failure");
			}

			return value;
		}
	}
}
