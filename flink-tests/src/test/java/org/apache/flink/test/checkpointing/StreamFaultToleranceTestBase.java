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

package org.apache.flink.test.checkpointing;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.fail;

/**
 * Test base for fault tolerant streaming programs
 */
public abstract class StreamFaultToleranceTestBase extends TestLogger {

	protected static final int NUM_TASK_MANAGERS = 2;
	protected static final int NUM_TASK_SLOTS = 3;
	protected static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
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
	public static void stopCluster() {
		try {
			cluster.stop();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}

	/**
	 * Implementations are expected to assemble the test topology in this function
	 * using the provided {@link StreamExecutionEnvironment}.
	 */
	abstract public void testProgram(StreamExecutionEnvironment env);

	/**
	 * Implementations are expected to provide test here to verify the correct behavior.
	 */
	abstract public void postSubmit() throws Exception ;

	/**
	 * Runs the following program the test program defined in {@link #testProgram(StreamExecutionEnvironment)}
	 * followed by the checks in {@link #postSubmit}.
	 */
	@Test
	public void runCheckpointedProgram() {
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getLeaderRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(500);
			env.getConfig().disableSysoutLogging();

			testProgram(env);

			env.execute();

			postSubmit();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Frequently used utilities
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	public static class PrefixCount implements Serializable {

		public String prefix;
		public String value;
		public long count;

		public PrefixCount() {}

		public PrefixCount(String prefix, String value, long count) {
			this.prefix = prefix;
			this.value = value;
			this.count = count;
		}

		@Override
		public String toString() {
			return prefix + " / " + value;
		}
	}
}
