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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ZooKeeperCheckpointIDCounter} in a ZooKeeper ensemble.
 */
public final class ZKCheckpointIDCounterMultiServersTest extends TestLogger {

	@Rule
	public ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	/**
	 * Tests that {@link ZooKeeperCheckpointIDCounter} can be recovered after a
	 * connection loss exception from ZooKeeper ensemble.
	 *
	 * <p>See also FLINK-14091.
	 */
	@Test
	public void testRecoveredAfterConnectionLoss() throws Exception {

		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);

		try {
			OneShotLatch connectionLossLatch = new OneShotLatch();
			OneShotLatch reconnectedLatch = new OneShotLatch();

			TestingLastStateConnectionStateListener listener = new TestingLastStateConnectionStateListener(
				connectionLossLatch,
				reconnectedLatch);

			ZooKeeperCheckpointIDCounter idCounter = new ZooKeeperCheckpointIDCounter(
				client,
				"/checkpoint-id-counter",
				listener);
			idCounter.start();

			AtomicLong localCounter = new AtomicLong(1L);

			assertThat(
				"ZooKeeperCheckpointIDCounter doesn't properly work.",
				idCounter.getAndIncrement(),
				is(localCounter.getAndIncrement()));

			zooKeeperResource.restart();

			connectionLossLatch.await();
			reconnectedLatch.await();

			assertThat(
				"ZooKeeperCheckpointIDCounter doesn't properly work after reconnected.",
				idCounter.getAndIncrement(),
				is(localCounter.getAndIncrement()));
		} finally {
			client.close();
		}
	}

	private static final class TestingLastStateConnectionStateListener extends DefaultLastStateConnectionStateListener {

		private final OneShotLatch connectionLossLatch;
		private final OneShotLatch reconnectedLatch;

		private TestingLastStateConnectionStateListener(OneShotLatch connectionLossLatch, OneShotLatch reconnectedLatch) {
			this.connectionLossLatch = connectionLossLatch;
			this.reconnectedLatch = reconnectedLatch;
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			super.stateChanged(client, newState);

			if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
				connectionLossLatch.trigger();
			}

			if (newState == ConnectionState.RECONNECTED) {
				reconnectedLatch.trigger();
			}
		}
	}

}
