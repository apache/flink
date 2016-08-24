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

package org.apache.flink.mesos.runtime.clusterframework.store;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.StateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.mesos.Protos;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A ZooKeeper-backed Mesos worker store.
 */
public class ZooKeeperMesosWorkerStore implements MesosWorkerStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMesosWorkerStore.class);

	private final Object startStopLock = new Object();

	/** Root store path in ZK. */
	private final String storePath;

	/** Client (not a namespace facade) */
	private final CuratorFramework client;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/** A persistent value of the assigned framework ID */
	private final SharedValue frameworkIdInZooKeeper;

	/** A persistent count of all tasks created, for generating unique IDs */
	private final SharedCount totalTaskCountInZooKeeper;

	/** A persistent store of serialized workers */
	private final ZooKeeperStateHandleStore<MesosWorkerStore.Worker> workersInZooKeeper;

	@SuppressWarnings("unchecked")
	ZooKeeperMesosWorkerStore(
		CuratorFramework client,
		String storePath,
		StateStorageHelper<MesosWorkerStore.Worker> stateStorage
	) throws Exception {
		checkNotNull(storePath, "storePath");
		checkNotNull(stateStorage, "stateStorage");

		// Keep a reference to the original client and not the namespace facade. The namespace
		// facade cannot be closed.
		this.client = checkNotNull(client, "client");
		this.storePath = storePath;

		// All operations will have the given path as root
		client.newNamespaceAwareEnsurePath(storePath).ensure(client.getZookeeperClient());
		CuratorFramework facade = client.usingNamespace(client.getNamespace() + storePath);

		// Track the assignd framework ID.
		frameworkIdInZooKeeper = new SharedValue(facade, "/frameworkId", new byte[0]);

		// Keep a count of all tasks created ever, as the basis for a unique ID.
		totalTaskCountInZooKeeper = new SharedCount(facade, "/count", 0);

		// Keep track of the workers in state handle storage.
		facade.newNamespaceAwareEnsurePath("/workers").ensure(client.getZookeeperClient());
		CuratorFramework storeFacade = client.usingNamespace(facade.getNamespace() + "/workers");

		// using late-binding as a workaround for shaded curator dependency of flink-runtime.
		this.workersInZooKeeper = ZooKeeperStateHandleStore.class
			.getConstructor(CuratorFramework.class, StateStorageHelper.class)
			.newInstance(storeFacade, stateStorage);
	}

	@Override
	public void start() throws Exception {
		synchronized (startStopLock) {
			if (!isRunning) {
				isRunning = true;
				frameworkIdInZooKeeper.start();
				totalTaskCountInZooKeeper.start();
			}
		}
	}

	public void stop(boolean cleanup) throws Exception {
		synchronized (startStopLock) {
			if (isRunning) {
				frameworkIdInZooKeeper.close();
				totalTaskCountInZooKeeper.close();

				if(cleanup) {
					workersInZooKeeper.removeAndDiscardAllState();
					client.delete().deletingChildrenIfNeeded().forPath(storePath);
				}

				client.close();
				isRunning = false;
			}
		}
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	/**
	 * Get the persisted framework ID.
	 * @return the current ID or empty if none is yet persisted.
	 * @throws Exception on ZK failures, interruptions.
	 */
	@Override
	public Option<Protos.FrameworkID> getFrameworkID() throws Exception {
		synchronized (startStopLock) {
			verifyIsRunning();

			Option<Protos.FrameworkID> frameworkID;
			byte[] value = frameworkIdInZooKeeper.getValue();
			if (value.length == 0) {
				frameworkID = Option.empty();
			} else {
				frameworkID = Option.apply(Protos.FrameworkID.newBuilder().setValue(new String(value)).build());
			}

			return frameworkID;
		}
	}

	/**
	 * Update the persisted framework ID.
	 * @param frameworkID the new ID or empty to remove the persisted ID.
	 * @throws Exception on ZK failures, interruptions.
	 */
	@Override
	public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception {
		synchronized (startStopLock) {
			verifyIsRunning();

			byte[] value = frameworkID.isDefined() ? frameworkID.get().getValue().getBytes() : new byte[0];
			frameworkIdInZooKeeper.setValue(value);
		}
	}

	/**
	 * Generates a new task ID.
	 */
	@Override
	public Protos.TaskID newTaskID() throws Exception {
		synchronized (startStopLock) {
			verifyIsRunning();

			int nextCount;
			boolean success;
			do {
				VersionedValue<Integer> count = totalTaskCountInZooKeeper.getVersionedValue();
				nextCount = count.getValue() + 1;
				success = totalTaskCountInZooKeeper.trySetCount(count, nextCount);
			}
			while (!success);

			Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASKID_FORMAT.format(nextCount)).build();
			return taskID;
		}
	}

	@Override
	public List<MesosWorkerStore.Worker> recoverWorkers() throws Exception {
		synchronized (startStopLock) {
			verifyIsRunning();

			List<Tuple2<StateHandle<MesosWorkerStore.Worker>, String>> handles = workersInZooKeeper.getAll();

			if(handles.size() != 0) {
				List<MesosWorkerStore.Worker> workers = new ArrayList<>(handles.size());
				for (Tuple2<StateHandle<MesosWorkerStore.Worker>, String> handle : handles) {
					Worker worker = handle.f0.getState(ClassLoader.getSystemClassLoader());

					workers.add(worker);
				}

				return workers;
			}
			else {
				return Collections.emptyList();
			}
		}
	}

	@Override
	public void putWorker(MesosWorkerStore.Worker worker) throws Exception {

		checkNotNull(worker, "worker");
		String path = getPathForWorker(worker.taskID());

		synchronized (startStopLock) {
			verifyIsRunning();

			int currentVersion = workersInZooKeeper.exists(path);
			if (currentVersion == -1) {
				try {
					workersInZooKeeper.add(path, worker);
					LOG.debug("Added {} in ZooKeeper.", worker);
				} catch (KeeperException.NodeExistsException ex) {
					throw new ConcurrentModificationException("ZooKeeper unexpectedly modified", ex);
				}
			} else {
				try {
					workersInZooKeeper.replace(path, currentVersion, worker);
					LOG.debug("Updated {} in ZooKeeper.", worker);
				} catch (KeeperException.NoNodeException ex) {
					throw new ConcurrentModificationException("ZooKeeper unexpectedly modified", ex);
				}
			}
		}
	}

	@Override
	public boolean removeWorker(Protos.TaskID taskID) throws Exception {
		checkNotNull(taskID, "taskID");
		String path = getPathForWorker(taskID);
		synchronized (startStopLock) {
			verifyIsRunning();

			if(workersInZooKeeper.exists(path) == -1) {
				LOG.debug("No such worker {} in ZooKeeper.", taskID);
				return false;
			}

			workersInZooKeeper.removeAndDiscardState(path);
			LOG.debug("Removed worker {} from ZooKeeper.", taskID);
			return true;
		}
	}

	/**
	 * Get the ZK path for the given task ID (with leading slash).
	 */
	private static String getPathForWorker(Protos.TaskID taskID) {
		checkNotNull(taskID, "taskID");
		return String.format("/%s", taskID.getValue());
	}

	/**
	 * Create the ZooKeeper-backed Mesos worker store.
	 * @param client the curator client.
	 * @param configuration the Flink configuration.
	 * @return a worker store.
	 * @throws Exception
	 */
	public static ZooKeeperMesosWorkerStore createMesosWorkerStore(
			CuratorFramework client,
			Configuration configuration) throws Exception {

		checkNotNull(configuration, "Configuration");

		StateStorageHelper<MesosWorkerStore.Worker> stateStorage =
			ZooKeeperUtils.createFileSystemStateStorage(configuration, "mesosWorkerStore");

		String zooKeeperMesosWorkerStorePath = configuration.getString(
			ConfigConstants.ZOOKEEPER_MESOS_WORKERS_PATH,
			ConfigConstants.DEFAULT_ZOOKEEPER_MESOS_WORKERS_PATH
		);

		return new ZooKeeperMesosWorkerStore(
			client, zooKeeperMesosWorkerStorePath, stateStorage);
	}
}
