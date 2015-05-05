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
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * A series of tests (reusing one FlinkMiniCluster) where tasks fail (one or more time)
 * and the recovery should restart them to verify job completion.
 */
@SuppressWarnings("serial")
public class SimpleRecoveryITCase {

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void setupCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "100 ms");

		cluster = new ForkableFlinkMiniCluster(config, false);
	}

	@AfterClass
	public static void teardownCluster() {
		try {
			cluster.stop();
		}
		catch (Throwable t) {
			System.err.println("Error stopping cluster on shutdown");
			t.printStackTrace();
			fail("Cluster shutdown caused an exception: " + t.getMessage());
		}
	}

	@Test
	public void testFailedRunThenSuccessfulRun() {

		try {
			List<Long> resultCollection = new ArrayList<Long>();

			// attempt 1
			{
				ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
						"localhost", cluster.getJobManagerRPCPort());

				env.setParallelism(4);
				env.setNumberOfExecutionRetries(0);
				env.getConfig().disableSysoutLogging();

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
					String msg = res == null ? "null result" : "result in " + res.getNetRuntime() + " ms";
					fail("The program should have failed, but returned " + msg);
				}
				catch (ProgramInvocationException e) {
					// expected
				}
			}

			// attempt 2
			{
				ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
						"localhost", cluster.getJobManagerRPCPort());

				env.setParallelism(4);
				env.setNumberOfExecutionRetries(0);
				env.getConfig().disableSysoutLogging();

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
					"localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(4);
			env.setNumberOfExecutionRetries(1);
			env.getConfig().disableSysoutLogging();

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
					"localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(4);
			env.setNumberOfExecutionRetries(5);
			env.getConfig().disableSysoutLogging();

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

		private static volatile int failuresBeforeSuccess = 1;

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

		private static volatile int failuresBeforeSuccess = 1;

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

		private static volatile int failuresBeforeSuccess = 3;

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
