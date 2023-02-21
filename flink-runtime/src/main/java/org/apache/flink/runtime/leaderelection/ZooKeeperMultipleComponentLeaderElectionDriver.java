/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheSelector;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** ZooKeeper based {@link MultipleComponentLeaderElectionDriver} implementation. */
public class ZooKeeperMultipleComponentLeaderElectionDriver
        implements MultipleComponentLeaderElectionDriver, LeaderLatchListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperMultipleComponentLeaderElectionDriver.class);

    private final CuratorFramework curatorFramework;

    private final MultipleComponentLeaderElectionDriver.Listener leaderElectionListener;

    private final LeaderLatch leaderLatch;

    private final TreeCache treeCache;

    private final ConnectionStateListener listener =
            (client, newState) -> handleStateChange(newState);

    private AtomicBoolean running = new AtomicBoolean(true);

    public ZooKeeperMultipleComponentLeaderElectionDriver(
            CuratorFramework curatorFramework,
            MultipleComponentLeaderElectionDriver.Listener leaderElectionListener)
            throws Exception {
        this.curatorFramework = Preconditions.checkNotNull(curatorFramework);
        this.leaderElectionListener = Preconditions.checkNotNull(leaderElectionListener);

        this.leaderLatch = new LeaderLatch(curatorFramework, ZooKeeperUtils.getLeaderLatchPath());
        this.treeCache =
                TreeCache.newBuilder(curatorFramework, "/")
                        .setCacheData(true)
                        .setCreateParentNodes(false)
                        .setSelector(
                                new ZooKeeperMultipleComponentLeaderElectionDriver
                                        .ConnectionInfoNodeSelector())
                        .setExecutor(Executors.newDirectExecutorService())
                        .build();
        treeCache
                .getListenable()
                .addListener(
                        (client, event) -> {
                            switch (event.getType()) {
                                case NODE_ADDED:
                                case NODE_UPDATED:
                                    Preconditions.checkNotNull(
                                            event.getData(),
                                            "The ZooKeeper event data must not be null.");
                                    handleChangedLeaderInformation(event.getData());
                                    break;
                                case NODE_REMOVED:
                                    Preconditions.checkNotNull(
                                            event.getData(),
                                            "The ZooKeeper event data must not be null.");
                                    handleRemovedLeaderInformation(event.getData().getPath());
                                    break;
                            }
                        });

        leaderLatch.addListener(this);
        curatorFramework.getConnectionStateListenable().addListener(listener);
        leaderLatch.start();
        treeCache.start();
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            curatorFramework.getConnectionStateListenable().removeListener(listener);

            Exception exception = null;

            try {
                treeCache.close();
            } catch (Exception e) {
                exception = e;
            }

            try {
                leaderLatch.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            ExceptionUtils.tryRethrowException(exception);
        }
    }

    @Override
    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
            throws Exception {
        Preconditions.checkState(running.get());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write leader information {} for {}.", leaderInformation, componentId);
        }

        if (!leaderLatch.hasLeadership()) {
            return;
        }

        final String connectionInformationPath =
                ZooKeeperUtils.generateConnectionInformationPath(componentId);

        ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                leaderInformation,
                curatorFramework,
                leaderLatch::hasLeadership,
                connectionInformationPath);
    }

    @Override
    public void deleteLeaderInformation(String leaderName) throws Exception {
        ZooKeeperUtils.deleteZNode(
                curatorFramework, ZooKeeperUtils.generateZookeeperPath(leaderName));
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended, waiting for reconnection.");
                break;
            case RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. Leader election can be restarted.");
                break;
            case LOST:
                // Maybe we have to throw an exception here to terminate the JobManager
                LOG.warn(
                        "Connection to ZooKeeper lost. The contender no longer participates in the leader election.");
                break;
        }
    }

    @Override
    public void isLeader() {
        LOG.debug("{} obtained the leadership.", this);
        leaderElectionListener.isLeader();
    }

    @Override
    public void notLeader() {
        LOG.debug("{} lost the leadership.", this);
        leaderElectionListener.notLeader();
    }

    private void handleChangedLeaderInformation(ChildData childData) {
        if (shouldHandleLeaderInformationEvent(childData.getPath())) {
            final String leaderName = extractLeaderName(childData.getPath());

            final LeaderInformation leaderInformation =
                    tryReadingLeaderInformation(childData, leaderName);

            leaderElectionListener.notifyLeaderInformationChange(leaderName, leaderInformation);
        }
    }

    private String extractLeaderName(String path) {
        final String[] splits = ZooKeeperUtils.splitZooKeeperPath(path);

        Preconditions.checkState(
                splits.length >= 2,
                String.format(
                        "Expecting path consisting of /<leader_name>/connection_info. Got path '%s'",
                        path));

        return splits[splits.length - 2];
    }

    private void handleRemovedLeaderInformation(String removedNodePath) {
        if (shouldHandleLeaderInformationEvent(removedNodePath)) {
            final String leaderName = extractLeaderName(removedNodePath);

            leaderElectionListener.notifyLeaderInformationChange(
                    leaderName, LeaderInformation.empty());
        }
    }

    private boolean shouldHandleLeaderInformationEvent(String path) {
        return running.get() && leaderLatch.hasLeadership() && isConnectionInfoNode(path);
    }

    private boolean isConnectionInfoNode(String path) {
        return path.endsWith(ZooKeeperUtils.CONNECTION_INFO_NODE);
    }

    private LeaderInformation tryReadingLeaderInformation(ChildData childData, String id) {
        LeaderInformation leaderInformation;
        try {
            leaderInformation = ZooKeeperUtils.readLeaderInformation(childData.getData());

            LOG.debug("Leader information for {} has changed to {}.", id, leaderInformation);
        } catch (IOException | ClassNotFoundException e) {
            LOG.debug(
                    "Could not read leader information for {}. Rewriting the information.", id, e);
            leaderInformation = LeaderInformation.empty();
        }

        return leaderInformation;
    }

    /**
     * This selector finds all connection info nodes. See {@link
     * org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperMultipleComponentLeaderElectionHaServices}
     * for more details on the Znode layout.
     */
    private static class ConnectionInfoNodeSelector implements TreeCacheSelector {
        @Override
        public boolean traverseChildren(String fullPath) {
            return true;
        }

        @Override
        public boolean acceptChild(String fullPath) {
            return !fullPath.endsWith(ZooKeeperUtils.getLeaderLatchPath());
        }
    }

    @Override
    public String toString() {
        return "ZooKeeperMultipleComponentLeaderElectionDriver";
    }
}
