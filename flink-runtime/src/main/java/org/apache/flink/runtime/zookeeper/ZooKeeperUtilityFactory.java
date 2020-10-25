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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.shared.SharedValue;

import java.io.Serializable;

/**
 * Creates ZooKeeper utility classes without exposing the {@link CuratorFramework} dependency. The
 * curator framework is cached in this instance and shared among all created ZooKeeper utility
 * instances. This requires that the utility classes DO NOT close the provided curator framework.
 *
 * <p>The curator framework is closed by calling the {@link #close(boolean)} method.
 */
public class ZooKeeperUtilityFactory {

	private final CuratorFramework root;

	// Facade bound to the provided path
	private final CuratorFramework facade;

	public ZooKeeperUtilityFactory(Configuration configuration, String path) throws Exception {
		Preconditions.checkNotNull(path, "path");

		root = ZooKeeperUtils.startCuratorFramework(configuration);

		root.newNamespaceAwareEnsurePath(path).ensure(root.getZookeeperClient());
		facade = root.usingNamespace(ZooKeeperUtils.generateZookeeperPath(root.getNamespace(), path));
	}

	/**
	 * Closes the ZooKeeperUtilityFactory. This entails closing the cached {@link CuratorFramework}
	 * instance. If cleanup is true, then the initial path and all its children are deleted.
	 *
	 * @param cleanup deletes the initial path and all of its children to clean up
	 * @throws Exception when deleting the znodes
	 */
	public void close(boolean cleanup) throws Exception {
		if (cleanup) {
			facade.delete().deletingChildrenIfNeeded().forPath("/");
		}

		root.close();
	}

	/**
	 * Creates a {@link ZooKeeperStateHandleStore} instance with the provided arguments.
	 *
	 * @param zkStateHandleStorePath specifying the path in ZooKeeper to store the state handles to
	 * @param stateStorageHelper storing the actual state data
	 * @param <T> Type of the state to be stored
	 * @return a ZooKeeperStateHandleStore instance
	 * @throws Exception if ZooKeeper could not create the provided state handle store path in
	 *     ZooKeeper
	 */
	public <T extends Serializable> ZooKeeperStateHandleStore<T> createZooKeeperStateHandleStore(
			String zkStateHandleStorePath,
			RetrievableStateStorageHelper<T> stateStorageHelper) throws Exception {

		return ZooKeeperUtils.createZooKeeperStateHandleStore(
			facade,
			zkStateHandleStorePath,
			stateStorageHelper);
	}

	/**
	 * Creates a {@link ZooKeeperSharedValue} to store a shared value between multiple instances.
	 *
	 * @param path to the shared value in ZooKeeper
	 * @param seedValue for the shared value
	 * @return a shared value
	 */
	public ZooKeeperSharedValue createSharedValue(String path, byte[] seedValue) {
		return new ZooKeeperSharedValue(
			new SharedValue(
				facade,
				path,
				seedValue));
	}

	/**
	 * Creates a {@link ZooKeeperSharedCount} to store a shared count between multiple instances.
	 *
	 * @param path to the shared count in ZooKeeper
	 * @param seedCount for the shared count
	 * @return a shared count
	 */
	public ZooKeeperSharedCount createSharedCount(String path, int seedCount) {
		return new ZooKeeperSharedCount(
			new SharedCount(
				facade,
				path,
				seedCount));
	}
}
