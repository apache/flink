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

package org.apache.flink.runtime.leaderretrieval;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.UUID;

/**
 * The counterpart to the {@link org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService}.
 * This implementation of the {@link LeaderRetrievalService} retrieves the current leader which has
 * been elected by the {@link org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService}.
 * The leader address as well as the current leader session ID is retrieved from ZooKeeper.
 */
public class ZooKeeperLeaderRetrievalService implements LeaderRetrievalService, NodeCacheListener {
	private static final Logger LOG = LoggerFactory.getLogger(
		ZooKeeperLeaderRetrievalService.class);

	/** Connection to the used ZooKeeper quorum */
	private final CuratorFramework client;

	/** Curator recipe to watch changes of a specific ZooKeeper node */
	private final NodeCache cache;

	/** Listener which will be notified about leader changes */
	private volatile LeaderRetrievalListener leaderListener;

	private String lastLeaderAddress;
	private UUID lastLeaderSessionID;

	/**
	 * Creates a leader retrieval service which uses ZooKeeper to retrieve the leader information.
	 *
	 * @param client Client which constitutes the connection to the ZooKeeper quorum
	 * @param retrievalPath Path of the ZooKeeper node which contains the leader information
	 */
	public ZooKeeperLeaderRetrievalService(CuratorFramework client, String retrievalPath) {
		this.client = client;
		this.cache = new NodeCache(client, retrievalPath);
	}

	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		Preconditions.checkNotNull(listener, "Listener must not be null.");
		Preconditions.checkState(leaderListener == null, "ZooKeeperLeaderRetrievalService can " +
				"only be started once.");

		LOG.info("Starting ZooKeeperLeaderRetrievalService.");

		leaderListener = listener;

		cache.getListenable().addListener(this);
		cache.start();
	}

	@Override
	public void stop() throws Exception {
		LOG.info("Stopping ZooKeeperLeaderRetrievalService.");

		cache.close();
		client.close();
	}

	@Override
	public void nodeChanged() throws Exception {
		try {
			ChildData childData = cache.getCurrentData();

			String leaderAddress;
			UUID leaderSessionID;

			if (childData == null) {
				leaderAddress = null;
				leaderSessionID = null;
			} else {
				byte[] data = childData.getData();

				if (data == null || data.length == 0) {
					leaderAddress = null;
					leaderSessionID = null;
				} else {
					ByteArrayInputStream bais = new ByteArrayInputStream(data);
					ObjectInputStream ois = new ObjectInputStream(bais);

					leaderAddress = ois.readUTF();
					leaderSessionID = (UUID) ois.readObject();
				}
			}

			if(!(Objects.equals(leaderAddress, lastLeaderAddress) &&
					Objects.equals(leaderSessionID, lastLeaderSessionID))) {
				lastLeaderAddress = leaderAddress;
				lastLeaderSessionID = leaderSessionID;
				leaderListener.notifyLeaderAddress(leaderAddress, leaderSessionID);
			}
		} catch (Exception e) {
			leaderListener.handleError(new Exception("Could not handle node changed event.", e));
			throw e;
		}
	}
}
