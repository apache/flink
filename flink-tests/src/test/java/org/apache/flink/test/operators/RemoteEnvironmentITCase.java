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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.StandaloneMiniCluster;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for {@link org.apache.flink.api.java.RemoteEnvironment}.
 */
@SuppressWarnings("serial")
public class RemoteEnvironmentITCase extends TestLogger {

	private static final int TM_SLOTS = 4;

	private static final int USER_DOP = 2;

	private static final String INVALID_STARTUP_TIMEOUT = "0.001 ms";

	private static final String VALID_STARTUP_TIMEOUT = "100 s";

	private static Configuration configuration;

	private static AutoCloseableAsync resource;

	private static String hostname;

	private static int port;

	@BeforeClass
	public static void setupCluster() throws Exception {
		configuration = new Configuration();

		if (CoreOptions.NEW_MODE.equals(configuration.getString(CoreOptions.MODE))) {
			configuration.setInteger(WebOptions.PORT, 0);
			final MiniCluster miniCluster = new MiniCluster(
				new MiniClusterConfiguration.Builder()
					.setConfiguration(configuration)
					.setNumSlotsPerTaskManager(TM_SLOTS)
					.build());

			miniCluster.start();

			final URI uri = miniCluster.getRestAddress();
			hostname = uri.getHost();
			port = uri.getPort();

			configuration.setInteger(WebOptions.PORT, port);

			resource = miniCluster;
		} else {
			configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, TM_SLOTS);
			final StandaloneMiniCluster standaloneMiniCluster = new StandaloneMiniCluster(configuration);
			hostname = standaloneMiniCluster.getHostname();
			port = standaloneMiniCluster.getPort();

			resource = standaloneMiniCluster;
		}
	}

	@AfterClass
	public static void tearDownCluster() throws Exception {
		resource.close();
	}

	/**
	 * Ensure that that Akka configuration parameters can be set.
	 */
	@Test(expected = FlinkException.class)
	public void testInvalidAkkaConfiguration() throws Throwable {
		assumeTrue(CoreOptions.LEGACY_MODE.equalsIgnoreCase(configuration.getString(CoreOptions.MODE)));
		Configuration config = new Configuration();
		config.setString(AkkaOptions.STARTUP_TIMEOUT, INVALID_STARTUP_TIMEOUT);

		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				hostname,
				port,
				config
		);
		env.getConfig().disableSysoutLogging();

		DataSet<String> result = env.createInput(new TestNonRichInputFormat());
		result.output(new LocalCollectionOutputFormat<>(new ArrayList<String>()));
		try {
			env.execute();
			Assert.fail("Program should not run successfully, cause of invalid akka settings.");
		} catch (ProgramInvocationException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Ensure that the program parallelism can be set even if the configuration is supplied.
	 */
	@Test
	public void testUserSpecificParallelism() throws Exception {
		Configuration config = new Configuration();
		config.setString(AkkaOptions.STARTUP_TIMEOUT, VALID_STARTUP_TIMEOUT);

		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
				hostname,
				port,
				config
		);
		env.setParallelism(USER_DOP);
		env.getConfig().disableSysoutLogging();

		DataSet<Integer> result = env.createInput(new ParallelismDependentInputFormat())
				.rebalance()
				.mapPartition(new RichMapPartitionFunction<Integer, Integer>() {
					@Override
					public void mapPartition(Iterable<Integer> values, Collector<Integer> out) throws Exception {
						out.collect(getRuntimeContext().getIndexOfThisSubtask());
					}
				});
		List<Integer> resultCollection = result.collect();
		assertEquals(USER_DOP, resultCollection.size());
	}

	private static class ParallelismDependentInputFormat extends GenericInputFormat<Integer> {

		private transient boolean emitted;

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
			assertEquals(USER_DOP, numSplits);
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
}
