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

package org.apache.flink.mesos.runtime.clusterframework.services;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.runtime.clusterframework.store.ZooKeeperMesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperSharedCount;
import org.apache.flink.runtime.zookeeper.ZooKeeperSharedValue;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.runtime.zookeeper.ZooKeeperUtilityFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;

import java.util.concurrent.Executor;

/**
 * {@link MesosServices} implementation for the ZooKeeper high availability based mode.
 */
public class ZooKeeperMesosServices extends AbstractMesosServices {

	// Factory to create ZooKeeper utility classes
	private final ZooKeeperUtilityFactory zooKeeperUtilityFactory;

	public ZooKeeperMesosServices(ActorSystem actorSystem, MesosArtifactServer artifactServer, ZooKeeperUtilityFactory zooKeeperUtilityFactory) {
		super(actorSystem, artifactServer);
		this.zooKeeperUtilityFactory = Preconditions.checkNotNull(zooKeeperUtilityFactory);
	}

	@Override
	public MesosWorkerStore createMesosWorkerStore(Configuration configuration, Executor executor) throws Exception {
		RetrievableStateStorageHelper<MesosWorkerStore.Worker> stateStorageHelper =
			ZooKeeperUtils.createFileSystemStateStorage(configuration, "mesosWorkerStore");

		ZooKeeperStateHandleStore<MesosWorkerStore.Worker> zooKeeperStateHandleStore = zooKeeperUtilityFactory.createZooKeeperStateHandleStore(
			"/workers",
			stateStorageHelper);

		ZooKeeperSharedValue frameworkId = zooKeeperUtilityFactory.createSharedValue("/frameworkId", new byte[0]);
		ZooKeeperSharedCount totalTaskCount = zooKeeperUtilityFactory.createSharedCount("/taskCount", 0);

		return new ZooKeeperMesosWorkerStore(
			zooKeeperStateHandleStore,
			frameworkId,
			totalTaskCount);
	}

	@Override
	public void close(boolean cleanup) throws Exception {
		Throwable exception = null;

		try {
			// this also closes the underlying CuratorFramework instance
			zooKeeperUtilityFactory.close(cleanup);
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		try {
			super.close(cleanup);
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the Mesos services.", exception);
		}
	}
}
