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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;

import javax.annotation.Nullable;

import java.util.List;

/** Simple ZooKeeper and CuratorFramework setup for tests. */
public class ZooKeeperTestEnvironment {

    private final TestingServer zooKeeperServer;

    private final TestingCluster zooKeeperCluster;

    private final CuratorFramework client;

    /**
     * Starts a ZooKeeper cluster with the number of quorum peers and a client.
     *
     * @param numberOfZooKeeperQuorumPeers Starts a {@link TestingServer}, if <code>1</code>. Starts
     *     a {@link TestingCluster}, if <code>=>1</code>.
     */
    public ZooKeeperTestEnvironment(int numberOfZooKeeperQuorumPeers) {
        if (numberOfZooKeeperQuorumPeers <= 0) {
            throw new IllegalArgumentException("Number of peers needs to be >= 1.");
        }

        final Configuration conf = new Configuration();

        try {
            if (numberOfZooKeeperQuorumPeers == 1) {
                zooKeeperServer = new TestingServer(true);
                zooKeeperCluster = null;

                conf.setString(
                        HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                        zooKeeperServer.getConnectString());
            } else {
                zooKeeperServer = null;
                zooKeeperCluster = new TestingCluster(numberOfZooKeeperQuorumPeers);

                zooKeeperCluster.start();

                conf.setString(
                        HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                        zooKeeperCluster.getConnectString());
            }

            client = ZooKeeperUtils.startCuratorFramework(conf);

            client.newNamespaceAwareEnsurePath("/").ensure(client.getZookeeperClient());
        } catch (Exception e) {
            throw new RuntimeException("Error setting up ZooKeeperTestEnvironment", e);
        }
    }

    /** Shutdown the client and ZooKeeper server/cluster. */
    public void shutdown() throws Exception {
        if (client != null) {
            client.close();
        }

        if (zooKeeperServer != null) {
            zooKeeperServer.close();
        }

        if (zooKeeperCluster != null) {
            zooKeeperCluster.close();
        }
    }

    public String getConnectString() {
        if (zooKeeperServer != null) {
            return zooKeeperServer.getConnectString();
        } else {
            return zooKeeperCluster.getConnectString();
        }
    }

    /** Returns a client for the started ZooKeeper server/cluster. */
    public CuratorFramework getClient() {
        return client;
    }

    public String getClientNamespace() {
        return client.getNamespace();
    }

    @Nullable
    public TestingCluster getZooKeeperCluster() {
        return zooKeeperCluster;
    }

    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    /** Creates a new client for the started ZooKeeper server/cluster. */
    public CuratorFramework createClient() {
        Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, getConnectString());
        return ZooKeeperUtils.startCuratorFramework(config);
    }

    /**
     * Deletes all ZNodes under the root node.
     *
     * @throws Exception If the ZooKeeper operation fails
     */
    public void deleteAll() throws Exception {
        final String path = "/" + client.getNamespace();

        int maxAttempts = 10;

        for (int i = 0; i < maxAttempts; i++) {
            try {
                ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, false);
                return;
            } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                // that seems all right. if one of the children we want to delete is
                // actually already deleted, that's fine.
                return;
            } catch (KeeperException.ConnectionLossException e) {
                // Keep retrying
                Thread.sleep(100);
            }
        }

        throw new Exception(
                "Could not clear the ZNodes under "
                        + path
                        + ". ZooKeeper is not in "
                        + "a clean state.");
    }
}
