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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
public class RemoteStreamExecutionEnvironmentTest extends TestLogger {

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(1)
			.build());

	/**
	 * Verifies that the port passed to the RemoteStreamEnvironment is used for connecting to the cluster.
	 */
	@Test
	public void testPortForwarding() throws Exception {
		final Configuration clientConfiguration = new Configuration();
		clientConfiguration.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 0);

		final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
			miniCluster.getRestAddress().getHost(),
			miniCluster.getRestAddress().getPort(),
			clientConfiguration);

		final DataStream<Integer> resultStream = env.fromElements(1)
			.map(x -> x * 2);

		final Iterator<Integer> result = DataStreamUtils.collect(resultStream);
		Assert.assertTrue(result.hasNext());
		Assert.assertEquals(2, result.next().intValue());
		Assert.assertFalse(result.hasNext());
	}
}
