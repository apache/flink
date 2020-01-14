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

import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.TestingCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ZooKeeperCheckpointIDCounter} in a ZooKeeper ensemble.
 */
public final class ZKCheckpointIDCounterMultiServersTest extends TestLogger {

	private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(3);

	@AfterClass
	public static void tearDown() throws Exception {
		ZOOKEEPER.shutdown();
	}

	@Before
	public void cleanUp() throws Exception {
		ZOOKEEPER.deleteAll();
	}

	/**
	 * Tests that {@link ZooKeeperCheckpointIDCounter} can be recovered after a
	 * connection loss exception from ZooKeeper ensemble.
	 *
	 * See also FLINK-14091.
	 */
	@Test
	public void testRecoveredAfterConnectionLoss() throws Exception {
		CuratorFramework client = ZOOKEEPER.getClient();

		ZooKeeperCheckpointIDCounter idCounter = new ZooKeeperCheckpointIDCounter(client, "/checkpoint-id-counter");
		idCounter.start();

		AtomicLong localCounter = new AtomicLong(1L);

		assertThat(
			"ZooKeeperCheckpointIDCounter doesn't properly work.",
			idCounter.getAndIncrement(),
			is(localCounter.getAndIncrement()));

		TestingCluster cluster = ZOOKEEPER.getZooKeeperCluster();
		assertThat(cluster, is(notNullValue()));

		// close the server this client connected to, which triggers a connection loss exception
		cluster.restartServer(cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper()));

		// encountered connected loss, this prevents us from getting false positive
		while (true) {
			try {
				idCounter.get();
			} catch (IllegalStateException ignore) {
				log.debug("Encountered connection loss.");
				break;
			}
		}

		// recovered from connection loss
		while (true) {
			try {
				long id = idCounter.get();
				assertThat(id, is(localCounter.get()));
				break;
			} catch (IllegalStateException ignore) {
				log.debug("During ZooKeeper client reconnecting...");
			}
		}

		assertThat(idCounter.getLastState(), is(ConnectionState.RECONNECTED));
		assertThat(
			"ZooKeeperCheckpointIDCounter doesn't properly work after reconnected.",
			idCounter.getAndIncrement(),
			is(localCounter.getAndIncrement()));
	}

}
