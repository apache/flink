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

package org.apache.flink.test.runtime;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;

/**
 * Test network stack with taskmanager.network.netty.transport set to epoll. This test can only run
 * on linux. On other platforms it's basically a NO-OP. See
 * https://github.com/apache/flink-shaded/issues/30
 */
public class NettyEpollITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(NettyEpollITCase.class);

	private static final int NUM_TASK_MANAGERS = 2;

	@Test
	public void testNettyEpoll() throws Exception {
		MiniClusterWithClientResource cluster = trySetUpCluster();
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_TASK_MANAGERS);

			DataStream<Integer> input = env.fromElements(1, 2, 3, 4, 1, 2, 3, 42);
			input.keyBy(new KeySelector<Integer, Integer>() {
					@Override
					public Integer getKey(Integer value) throws Exception {
						return value;
					}
				})
				.sum(0)
				.print();

			env.execute();
		}
		finally {
			cluster.after();
		}
	}

	private MiniClusterWithClientResource trySetUpCluster() throws Exception {
		try {
			Configuration config = new Configuration();
			config.setString(NettyShuffleEnvironmentOptions.TRANSPORT_TYPE, "epoll");
			MiniClusterWithClientResource cluster = new MiniClusterWithClientResource(
				new MiniClusterResourceConfiguration.Builder()
					.setConfiguration(config)
					.setNumberTaskManagers(NUM_TASK_MANAGERS)
					.setNumberSlotsPerTaskManager(1)
					.build());
			cluster.before();
			return cluster;
		}
		catch (UnsatisfiedLinkError ex) {
			// If we failed to init netty because we are not on Linux platform, abort the test.
			if (findThrowableWithMessage(ex, "Only supported on Linux").isPresent()) {
				throw new AssumptionViolatedException("This test is only supported on linux");
			}
			throw ex;
		}
	}
}
