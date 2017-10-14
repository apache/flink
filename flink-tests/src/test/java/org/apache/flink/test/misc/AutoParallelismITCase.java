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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test verifies that the auto parallelism is properly forwarded to the runtime.
 */
@SuppressWarnings("serial")
public class AutoParallelismITCase extends TestLogger {

	private static final int NUM_TM = 2;
	private static final int SLOTS_PER_TM = 7;
	private static final int PARALLELISM = NUM_TM * SLOTS_PER_TM;

	private static LocalFlinkMiniCluster cluster;

	private static TestEnvironment env;

	@BeforeClass
	public static void setupCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TM);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, SLOTS_PER_TM);
		cluster = new LocalFlinkMiniCluster(config, false);

		cluster.start();

		env = new TestEnvironment(cluster, NUM_TM * SLOTS_PER_TM, false);
	}

	@AfterClass
	public static void teardownCluster() {
		try {
			cluster.stop();
		}
		catch (Throwable t) {
			System.err.println("Error stopping cluster on shutdown");
			t.printStackTrace();
			fail("ClusterClient shutdown caused an exception: " + t.getMessage());
		}
	}

	@Test
	public void testProgramWithAutoParallelism() {
		try {
			env.setParallelism(ExecutionConfig.PARALLELISM_AUTO_MAX);
			env.getConfig().disableSysoutLogging();

			DataSet<Integer> result = env
					.createInput(new ParallelismDependentInputFormat())
					.rebalance()
					.mapPartition(new ParallelismDependentMapPartition());

			List<Integer> resultCollection = new ArrayList<Integer>();
			result.output(new LocalCollectionOutputFormat<Integer>(resultCollection));

			env.execute();

			assertEquals(PARALLELISM, resultCollection.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static class ParallelismDependentInputFormat extends GenericInputFormat<Integer> {

		private transient boolean emitted;

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
			assertEquals(PARALLELISM, numSplits);
			return super.createInputSplits(numSplits);
		}

		@Override
		public boolean reachedEnd() {
			return emitted;
		}

		@Override
		public Integer nextRecord(Integer reuse) {
			if (emitted) {
				return null;
			}
			emitted = true;
			return 1;
		}
	}

	private static class ParallelismDependentMapPartition extends RichMapPartitionFunction<Integer, Integer> {

		@Override
		public void mapPartition(Iterable<Integer> values, Collector<Integer> out) {
			out.collect(getRuntimeContext().getIndexOfThisSubtask());
		}
	}
}
