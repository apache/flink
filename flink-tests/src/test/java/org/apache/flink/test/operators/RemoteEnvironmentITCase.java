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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link org.apache.flink.api.java.RemoteEnvironment}.
 */
@SuppressWarnings("serial")
public class RemoteEnvironmentITCase extends TestLogger {

	private static final int TM_SLOTS = 4;

	private static final int USER_DOP = 2;

	private static final String VALID_STARTUP_TIMEOUT = "100 s";

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberSlotsPerTaskManager(TM_SLOTS)
			.build());

	/**
	 * Ensure that the program parallelism can be set even if the configuration is supplied.
	 */
	@Test
	public void testUserSpecificParallelism() throws Exception {
		Configuration config = new Configuration();
		config.setString(AkkaOptions.STARTUP_TIMEOUT, VALID_STARTUP_TIMEOUT);

		final URI restAddress = MINI_CLUSTER_RESOURCE.getRestAddres();
		final String hostname = restAddress.getHost();
		final int port = restAddress.getPort();

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
