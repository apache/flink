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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.apache.flink.util.Collector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for the system behavior in multiple corner cases
 *   - when null records are passed through the system.
 *   - when disjoint dataflows are executed
 *   - when accumulators are used chained after a non-udf operator.
 *   
 * The tests are bundled into one class to reuse the same test cluster. This speeds
 * up test execution, as the majority of the test time goes usually into starting/stopping the
 * test cluster.
 */
@SuppressWarnings("serial")
public class MiscellaneousIssuesITCase {

	private static ForkableFlinkMiniCluster cluster;
	
	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 3);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);
			cluster = new ForkableFlinkMiniCluster(config, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}
	
	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}
	
	@Test
	public void testNullValues() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(1);
			env.getConfig().disableSysoutLogging();

			DataSet<String> data = env.fromElements("hallo")
					.map(new MapFunction<String, String>() {
						@Override
						public String map(String value) throws Exception {
							return null;
						}
					});
			data.writeAsText("/tmp/myTest", FileSystem.WriteMode.OVERWRITE);

			try {
				env.execute();
				fail("this should fail due to null values.");
			}
			catch (ProgramInvocationException e) {
				assertNotNull(e.getCause());
				assertNotNull(e.getCause().getCause());
				assertTrue(e.getCause().getCause() instanceof NullPointerException);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDisjointDataflows() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(5);
			env.getConfig().disableSysoutLogging();

			// generate two different flows
			env.generateSequence(1, 10).output(new DiscardingOutputFormat<Long>());
			env.generateSequence(1, 10).output(new DiscardingOutputFormat<Long>());

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAccumulatorsAfterNoOp() {
		
		final String ACC_NAME = "test_accumulator";
		
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getJobManagerRPCPort());

			env.setParallelism(6);
			env.getConfig().disableSysoutLogging();
			
			env.generateSequence(1, 1000000)
					.rebalance()
					.flatMap(new RichFlatMapFunction<Long, Long>() {

						private LongCounter counter;

						@Override
						public void open(Configuration parameters) {
							counter = getRuntimeContext().getLongCounter(ACC_NAME);
						}

						@Override
						public void flatMap(Long value, Collector<Long> out) {
							counter.add(1L);
						}
					})
					.output(new DiscardingOutputFormat<Long>());

			JobExecutionResult result = env.execute();
			
			assertEquals(1000000L, result.getAllAccumulatorResults().get(ACC_NAME));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
