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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
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

	private final Collection<ConnectionStateListener> connectionStateListeners;

	private final Object startStopLock = new Object();

	@Nullable
	private volatile ConnectionState lastState;

	@GuardedBy("startStopLock")
	private boolean isStarted;

	/**
	 * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
	 *
	 * @param client      Curator ZooKeeper client
	 * @param counterPath ZooKeeper path for the counter. It's sufficient to have a path per-job.
	 */
	public ZooKeeperCheckpointIDCounter(CuratorFramework client, String counterPath) {
		this.client = checkNotNull(client, "Curator client");
		this.counterPath = checkNotNull(counterPath, "Counter path");
		this.sharedCount = new SharedCount(client, counterPath, 1);

		this.connectionStateListeners = new ArrayList<>();
		this.connectionStateListeners.add((ignore, newState) -> lastState = newState);
	}

	@VisibleForTesting
	ZooKeeperCheckpointIDCounter(CuratorFramework client, String counterPath, Collection<ConnectionStateListener> listeners) {
		this(client, counterPath);
		this.connectionStateListeners.addAll(listeners);
	}

	@Override
	public void start() throws Exception {
		synchronized (startStopLock) {
			if (!isStarted) {
				sharedCount.start();

				for (ConnectionStateListener listener : connectionStateListeners) {
					client.getConnectionStateListenable().addListener(listener);
				}

				isStarted = true;
			}
		}
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		synchronized (startStopLock) {
			if (isStarted) {
				LOG.info("Shutting down.");
				sharedCount.close();

				for (ConnectionStateListener listener : connectionStateListeners) {
					client.getConnectionStateListenable().removeListener(listener);
				}

				if (jobStatus.isGloballyTerminalState()) {
					LOG.info("Removing {} from ZooKeeper", counterPath);
					client.delete().deletingChildrenIfNeeded().inBackground().forPath(counterPath);
				}

				isStarted = false;
			}
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (true) {
			checkConnectionState();

			VersionedValue<Integer> current = sharedCount.getVersionedValue();
			int newCount = current.getValue() + 1;

			if (newCount < 0) {
				// overflow and wrap around
				throw new Exception("Checkpoint counter overflow. ZooKeeper checkpoint counter only supports " +
						"checkpoints Ids up to " + Integer.MAX_VALUE);
			}

			if (sharedCount.trySetCount(current, newCount)) {
				return current.getValue();
			}
		}
	}

	@Override
	public long get() {
		checkConnectionState();

		return sharedCount.getVersionedValue().getValue();
	}

	@Override
	public void setCount(long newId) throws Exception {
		checkConnectionState();

		if (newId > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("ZooKeeper checkpoint counter only supports " +
					"checkpoints Ids up to " + Integer.MAX_VALUE  + ", but given value is" +
					newId);
		}

		sharedCount.setCount((int) newId);
	}

	private void checkConnectionState() {
		final ConnectionState currentLastState = this.lastState;

		if (currentLastState == null) {
			return;
		}

		if (currentLastState != ConnectionState.CONNECTED && currentLastState != ConnectionState.RECONNECTED) {
			throw new IllegalStateException("Connection state: " + currentLastState);
		}
	}
}
