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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in {@link RecoveryMode#ZOOKEEPER}.
 *
 * <p>Each counter creates a ZNode:
 * <pre>
 * +----O /flink/checkpoint-counter/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/checkpoint-counter/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The checkpoints IDs are required to be ascending (per job). In order to guarantee this in case
 * of job manager failures we use ZooKeeper to have a shared counter across job manager instances.
 */
public class ZooKeeperCheckpointIDCounter implements CheckpointIDCounter {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCheckpointIDCounter.class);

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	/** Path of the shared count */
	private final String counterPath;

	/** Curator recipe for shared counts */
	private final SharedCount sharedCount;

	/** Connection state listener to monitor the client connection */
	private final SharedCountConnectionStateListener connStateListener =
			new SharedCountConnectionStateListener();

	/**
	 * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
	 *
	 * @param client      Curator ZooKeeper client
	 * @param counterPath ZooKeeper path for the counter. It's sufficient to have a path per-job.
	 * @throws Exception
	 */
	public ZooKeeperCheckpointIDCounter(CuratorFramework client, String counterPath) throws Exception {
		this.client = checkNotNull(client, "Curator client");
		this.counterPath = checkNotNull(counterPath, "Counter path");
		this.sharedCount = new SharedCount(client, counterPath, 1);
	}

	@Override
	public void start() throws Exception {
		sharedCount.start();
		client.getConnectionStateListenable().addListener(connStateListener);
	}

	@Override
	public void stop() throws Exception {
		sharedCount.close();
		client.getConnectionStateListenable().removeListener(connStateListener);

		LOG.info("Removing {} from ZooKeeper", counterPath);
		client.delete().deletingChildrenIfNeeded().inBackground().forPath(counterPath);
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (true) {
			ConnectionState connState = connStateListener.getLastState();

			if (connState != null) {
				throw new IllegalStateException("Connection state: " + connState);
			}

			VersionedValue<Integer> current = sharedCount.getVersionedValue();

			Integer newCount = current.getValue() + 1;

			if (sharedCount.trySetCount(current, newCount)) {
				return current.getValue();
			}
		}
	}
	
	@Override
	public long get() throws Exception {
		ConnectionState connState = connStateListener.getLastState();

		if (connState != null) {
			throw new IllegalStateException("Connection state: " + connState);
		}

		return sharedCount.getVersionedValue().getValue();
	}

	/**
	 * Connection state listener. In case of {@link ConnectionState#SUSPENDED} or {@link
	 * ConnectionState#LOST} we are not guaranteed to read a current count from ZooKeeper.
	 */
	private class SharedCountConnectionStateListener implements ConnectionStateListener {

		private volatile ConnectionState lastState;

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
				lastState = newState;
			}
		}

		private ConnectionState getLastState() {
			return lastState;
		}
	}
}
