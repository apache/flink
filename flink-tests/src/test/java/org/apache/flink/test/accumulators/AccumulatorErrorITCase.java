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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests cases where accumulators:
 *  a) throw errors during runtime
 *  b) are not compatible with existing accumulator.
 */
public class AccumulatorErrorITCase extends TestLogger {

	private static LocalFlinkMiniCluster cluster;

	private static ExecutionEnvironment env;

	@BeforeClass
	public static void startCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 3);
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 12L);
		cluster = new LocalFlinkMiniCluster(config, false);

		cluster.start();

		env = new TestEnvironment(cluster, 6, false);
	}

	@AfterClass
	public static void shutdownCluster() {
		cluster.stop();
		cluster = null;
	}

	@Test
	public void testFaultyAccumulator() throws Exception {

		env.getConfig().disableSysoutLogging();

		// Test Exception forwarding with faulty Accumulator implementation
		DataSet<Long> input = env.generateSequence(0, 10000);

		DataSet<Long> map = input.map(new FaultyAccumulatorUsingMapper());

		map.output(new DiscardingOutputFormat<Long>());

		try {
			env.execute();
			fail("Should have failed.");
		} catch (JobExecutionException e) {
			Assert.assertTrue("Root cause should be:",
					e.getCause() instanceof CustomException);
		}
	}

	@Test
	public void testInvalidTypeAccumulator() throws Exception {

		env.getConfig().disableSysoutLogging();

		// Test Exception forwarding with faulty Accumulator implementation
		DataSet<Long> input = env.generateSequence(0, 10000);

		DataSet<Long> mappers = input.map(new IncompatibleAccumulatorTypesMapper())
				.map(new IncompatibleAccumulatorTypesMapper2());

		mappers.output(new DiscardingOutputFormat<Long>());

		try {
			env.execute();
			fail("Should have failed.");
		} catch (JobExecutionException e) {
			Assert.assertTrue("Root cause should be:",
					e.getCause() instanceof Exception);
			Assert.assertTrue("Root cause should be:",
					e.getCause().getCause() instanceof UnsupportedOperationException);
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
