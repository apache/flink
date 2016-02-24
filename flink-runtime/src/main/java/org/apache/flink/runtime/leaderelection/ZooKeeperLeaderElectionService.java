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

package org.apache.flink.runtime.leaderelection;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * ZooKeeper. The current leader's address as well as its leader session ID is published via
 * ZooKeeper as well.
 */
public class ZooKeeperLeaderElectionService implements LeaderElectionService, LeaderLatchListener, NodeCacheListener {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionService.class);

	/** Client to the ZooKeeper quorum */
	private final CuratorFramework client;

	/** Curator recipe for leader election */
	private final LeaderLatch leaderLatch;

	/** Curator recipe to watch a given ZooKeeper node for changes */
	private final NodeCache cache;

	/** ZooKeeper path of the node which stores the current leader information */
	private final String leaderPath;

	private UUID issuedLeaderSessionID;

	private UUID confirmedLeaderSessionID;

	/** The leader contender which applies for leadership */
	private volatile LeaderContender leaderContender;

	private final Object lock = new Object();

	/**
	 * Creates a ZooKeeperLeaderElectionService object.
	 *
	 * @param client Client which is connected to the ZooKeeper quorum
	 * @param latchPath ZooKeeper node path for the leader election latch
	 * @param leaderPath ZooKeeper node path for the node which stores the current leader information
	 */
	public ZooKeeperLeaderElectionService(CuratorFramework client, String latchPath, String leaderPath) {
		this.client = client;
		this.leaderPath = leaderPath;

		leaderLatch = new LeaderLatch(client, latchPath);
		cache = new NodeCache(client, leaderPath);
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	@Override
	public void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		LOG.info("Starting ZooKeeperLeaderElectionService.");

		leaderContender = contender;

		leaderLatch.addListener(this);
		leaderLatch.start();

		cache.getListenable().addListener(this);
		cache.start();
	}

	@Override
	public void stop() throws Exception{
		LOG.info("Stopping ZooKeeperLeaderElectionService.");

		cache.close();
		leaderLatch.close();
		client.close();

		synchronized (lock) {
			confirmedLeaderSessionID = null;
			issuedLeaderSessionID = null;
		}
	}

	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(
				"Confirm leader session ID {} for leader {}.",
				leaderSessionID,
				leaderContender.getAddress());
		}

		Preconditions.checkNotNull(leaderSessionID);

		if(leaderLatch.hasLeadership()) {
			// check if this is an old confirmation call
			synchronized (lock) {
				if (leaderSessionID.equals(this.issuedLeaderSessionID)) {
					confirmedLeaderSessionID = leaderSessionID;
					writeLeaderInformation(confirmedLeaderSessionID);
				}
			}
		} else {
			LOG.warn("The leader session ID {} was confirmed even though the" +
					"corresponding JobManager was not elected as the leader.", leaderSessionID);
		}
	}

	@Override
	public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
	}

	@Override
	public void isLeader() {
		synchronized (lock) {
			issuedLeaderSessionID = UUID.randomUUID();
			confirmedLeaderSessionID = null;

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Grant leadership to contender {} with session ID {}.",
					leaderContender.getAddress(),
					issuedLeaderSessionID);
			}

			leaderContender.grantLeadership(issuedLeaderSessionID);
		}
	}

	@Override
	public void notLeader() {
		synchronized (lock) {
			issuedLeaderSessionID = null;
			confirmedLeaderSessionID = null;

			if (LOG.isDebugEnabled()) {
				LOG.debug("Revoke leadership of {}.", leaderContender.getAddress());
			}

			leaderContender.revokeLeadership();
		}
	}

	@Override
	public void nodeChanged() throws Exception {
		try {
			// leaderSessionID is null if the leader contender has not yet confirmed the session ID
			if (leaderLatch.hasLeadership()) {
				synchronized (lock) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(
							"Leader node changed while {} is the leader with session ID {}.",
							leaderContender.getAddress(),
							confirmedLeaderSessionID);
					}

					if (confirmedLeaderSessionID != null) {
						ChildData childData = cache.getCurrentData();

						if (childData == null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug(
									"Writing leader information into empty node by {}.",
									leaderContender.getAddress());
							}
							writeLeaderInformation(confirmedLeaderSessionID);
						} else {
							byte[] data = childData.getData();

							if (data == null || data.length == 0) {
								// the data field seems to be empty, rewrite information
								if (LOG.isDebugEnabled()) {
									LOG.debug(
										"Writing leader information into node with empty data field by {}.",
										leaderContender.getAddress());
								}
								writeLeaderInformation(confirmedLeaderSessionID);
							} else {
								ByteArrayInputStream bais = new ByteArrayInputStream(data);
								ObjectInputStream ois = new ObjectInputStream(bais);

								String leaderAddress = ois.readUTF();
								UUID leaderSessionID = (UUID) ois.readObject();

								if (!leaderAddress.equals(this.leaderContender.getAddress()) ||
										(leaderSessionID == null || !leaderSessionID.equals(confirmedLeaderSessionID))) {
									// the data field does not correspond to the expected leader information
									if (LOG.isDebugEnabled()) {
										LOG.debug(
											"Correcting leader information by {}.",
											leaderContender.getAddress());
									}
									writeLeaderInformation(confirmedLeaderSessionID);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not handle node changed event.", e));
			throw e;
		}
	}

	/**
	 * Writes the current leader's address as well the given leader session ID to ZooKeeper.
	 *
	 * @param leaderSessionID Leader session ID which is written to ZooKeeper
	 */
	protected void writeLeaderInformation(UUID leaderSessionID) {
		// this method does not have to be synchronized because the curator framework client
		// is thread-safe
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Write leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(leaderContender.getAddress());
			oos.writeObject(leaderSessionID);

			oos.close();

			boolean dataWritten = false;

			while(!dataWritten && leaderLatch.hasLeadership()) {
				Stat stat = client.checkExists().forPath(leaderPath);

				if (stat != null) {
					long owner = stat.getEphemeralOwner();
					long sessionID = client.getZookeeperClient().getZooKeeper().getSessionId();

					if(owner == sessionID) {
						try {
							client.setData().forPath(leaderPath, baos.toByteArray());

							dataWritten = true;
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime
						}
					} else {
						try {
							client.delete().forPath(leaderPath);
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime --> try again
						}
					}
				} else {
					try {
						client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
								leaderPath,
								baos.toByteArray());

						dataWritten = true;
					} catch (KeeperException.NodeExistsException nodeExists) {
						// node has been created in the meantime --> try again
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Successfully wrote leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
		} catch (Exception e) {
			leaderContender.handleError(
					new Exception("Could not write leader address and leader session ID to " +
							"ZooKeeper.", e));
		}
	}
}
