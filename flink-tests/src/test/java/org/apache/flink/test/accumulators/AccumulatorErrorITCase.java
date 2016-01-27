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

package org.apache.flink.test.accumulators;

import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import static org.junit.Assert.fail;

/**
 * Tests cases where Accumulator are
 *  a) throw errors during runtime
 *  b) is not compatible with existing accumulator
 */
public class AccumulatorErrorITCase {

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 3);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);
			cluster = new ForkableFlinkMiniCluster(config, false);

			cluster.start();
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
	public void testFaultyAccumulator() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());
		env.getConfig().disableSysoutLogging();

		// Test Exception forwarding with faulty Accumulator implementation
		DataSet<Long> input = env.generateSequence(0, 10000);

		DataSet<Long> map = input.map(new FaultyAccumulatorUsingMapper());

		map.output(new DiscardingOutputFormat<Long>());

		try {
			env.execute();
			fail("Should have failed.");
		} catch (ProgramInvocationException e) {
			Assert.assertTrue("Exception should be passed:",
					e.getCause() instanceof JobExecutionException);
			Assert.assertTrue("Root cause should be:",
					e.getCause().getCause() instanceof CustomException);
		}
	}


	@Test
	public void testInvalidTypeAccumulator() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());
		env.getConfig().disableSysoutLogging();

		// Test Exception forwarding with faulty Accumulator implementation
		DataSet<Long> input = env.generateSequence(0, 10000);

		DataSet<Long> mappers = input.map(new IncompatibleAccumulatorTypesMapper())
				.map(new IncompatibleAccumulatorTypesMapper2());

		mappers.output(new DiscardingOutputFormat<Long>());

		try {
			env.execute();
			fail("Should have failed.");
		} catch (ProgramInvocationException e) {
			Assert.assertTrue("Exception should be passed:",
					e.getCause() instanceof JobExecutionException);
			Assert.assertTrue("Root cause should be:",
					e.getCause().getCause() instanceof Exception);
			Assert.assertTrue("Root cause should be:",
					e.getCause().getCause().getCause() instanceof UnsupportedOperationException);
		}
	}

	/* testFaultyAccumulator */

	private static class FaultyAccumulatorUsingMapper extends RichMapFunction<Long, Long> {

		private static final long serialVersionUID = 42;

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator("test", new FaultyAccumulator());
		}

		@Override
		public Long map(Long value) throws Exception {
			return -1L;
		}
	}

	private static class FaultyAccumulator extends LongCounter {

		private static final long serialVersionUID = 42;

		@Override
		public LongCounter clone() {
			throw new CustomException();
		}
	}

	private static class CustomException extends RuntimeException {
		private static final long serialVersionUID = 42;
	}

	/* testInvalidTypeAccumulator */

	private static class IncompatibleAccumulatorTypesMapper extends RichMapFunction<Long, Long> {

		private static final long serialVersionUID = 42;

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator("test", new LongCounter());
		}

		@Override
		public Long map(Long value) throws Exception {
			return -1L;
		}
	}

	private static class IncompatibleAccumulatorTypesMapper2 extends RichMapFunction<Long, Long> {

		private static final long serialVersionUID = 42;

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator("test", new DoubleCounter());
		}

		@Override
		public Long map(Long value) throws Exception {
			return -1L;
		}
	}

}
