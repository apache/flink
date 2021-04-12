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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link AbstractHaServices} using Apache ZooKeeper. The services store
 * data in ZooKeeper's nodes as illustrated by the following tree structure:
 *
 * <pre>
 * /flink
 *      +/cluster_id_1/resource_manager_lock
 *      |            |
 *      |            +/job-id-1/job_manager_lock
 *      |            |         /checkpoints/latest
 *      |            |                     /latest-1
 *      |            |                     /latest-2
 *      |            |
 *      |            +/job-id-2/job_manager_lock
 *      |
 *      +/cluster_id_2/resource_manager_lock
 *                   |
 *                   +/job-id-1/job_manager_lock
 *                            |/checkpoints/latest
 *                            |            /latest-1
 *                            |/persisted_job_graph
 * </pre>
 *
 * <p>The root path "/flink" is configurable via the option {@link
 * HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}. This makes sure Flink stores its data under specific
 * subtrees in ZooKeeper, for example to accommodate specific permission.
 *
 * <p>The "cluster_id" part identifies the data stored for a specific Flink "cluster". This
 * "cluster" can be either a standalone or containerized Flink cluster, or it can be job on a
 * framework like YARN or Mesos (in a "per-job-cluster" mode).
 *
 * <p>In case of a "per-job-cluster" on YARN or Mesos, the cluster-id is generated and configured
 * automatically by the client or dispatcher that submits the Job to YARN or Mesos.
 *
 * <p>In the case of a standalone cluster, that cluster-id needs to be configured via {@link
 * HighAvailabilityOptions#HA_CLUSTER_ID}. All nodes with the same cluster id will join the same
 * cluster and participate in the execution of the same set of jobs.
 */
public class ZooKeeperHaServices extends AbstractHaServices {

    private static final String RESOURCE_MANAGER_LEADER_PATH = "/resource_manager_lock";

    private static final String DISPATCHER_LEADER_PATH = "/dispatcher_lock";

    private static final String JOB_MANAGER_LEADER_PATH = "/job_manager_lock";

    private static final String REST_SERVER_LEADER_PATH = "/rest_server_lock";

    // ------------------------------------------------------------------------

    /** The ZooKeeper client to use. */
    private final CuratorFramework client;

    public ZooKeeperHaServices(
            CuratorFramework client,
            Executor executor,
            Configuration configuration,
            BlobStoreService blobStoreService) {
        super(configuration, executor, blobStoreService);
        this.client = checkNotNull(client);
    }

    @Override
    public CheckpointRecoveryFactory createCheckpointRecoveryFactory() {
        return new ZooKeeperCheckpointRecoveryFactory(client, configuration, ioExecutor);
    }

    @Override
    public JobGraphStore createJobGraphStore() throws Exception {
        return ZooKeeperUtils.createJobGraphs(client, configuration);
    }

    @Override
    public RunningJobsRegistry createRunningJobsRegistry() {
        return new ZooKeeperRunningJobsRegistry(client, configuration);
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderName) {
        return ZooKeeperUtils.createLeaderElectionService(client, configuration, leaderName);
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderName) {
        return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, leaderName);
    }

    @Override
    public void internalClose() {
        client.close();
    }

    @Override
    public void internalCleanup() throws Exception {
        cleanupZooKeeperPaths();
    }

    @Override
    public void internalCleanupJobData(JobID jobID) throws Exception {
        final List<String> paths =
                Stream.of(
                                HighAvailabilityOptions.HA_ZOOKEEPER_CHECKPOINTS_PATH,
                                HighAvailabilityOptions.HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH,
                                HighAvailabilityOptions.HA_ZOOKEEPER_LATCH_PATH,
                                HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH)
                        .map(configuration::getString)
                        .map(parent -> parent + "/" + jobID)
                        .collect(Collectors.toList());
        for (String path : paths) {
            deleteZNode(path);
        }
    }

    @Override
    protected String getLeaderNameForResourceManager() {
        return RESOURCE_MANAGER_LEADER_PATH;
    }

    @Override
    protected String getLeaderNameForDispatcher() {
        return DISPATCHER_LEADER_PATH;
    }

    public String getLeaderNameForJobManager(final JobID jobID) {
        return "/" + jobID + JOB_MANAGER_LEADER_PATH;
    }

    @Override
    protected String getLeaderNameForRestServer() {
        return REST_SERVER_LEADER_PATH;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** Cleans up leftover ZooKeeper paths. */
    private void cleanupZooKeeperPaths() throws Exception {
        deleteOwnedZNode();
        tryDeleteEmptyParentZNodes();
    }

    private void deleteOwnedZNode() throws Exception {
        deleteZNode("/");
    }

    private void deleteZNode(String path) throws Exception {
        // delete the HA_CLUSTER_ID znode which is owned by this cluster

        // Since we are using Curator version 2.12 there is a bug in deleting the children
        // if there is a concurrent delete operation. Therefore we need to add this retry
        // logic. See https://issues.apache.org/jira/browse/CURATOR-430 for more information.
        // The retry logic can be removed once we upgrade to Curator version >= 4.0.1.
        boolean zNodeDeleted = false;
        while (!zNodeDeleted) {
            Stat stat = client.checkExists().forPath(path);
            if (stat == null) {
                logger.debug("znode {} has been deleted", path);
                return;
            }
            try {
                client.delete().deletingChildrenIfNeeded().forPath(path);
                zNodeDeleted = true;
            } catch (KeeperException.NoNodeException ignored) {
                // concurrent delete operation. Try again.
                logger.debug(
                        "Retrying to delete znode because of other concurrent delete operation.");
            }
        }
    }

    /**
     * Tries to delete empty parent znodes.
     *
     * <p>IMPORTANT: This method can be removed once all supported ZooKeeper versions support the
     * container {@link org.apache.zookeeper.CreateMode}.
     *
     * @throws Exception if the deletion fails for other reason than {@link
     *     KeeperException.NotEmptyException}
     */
    private void tryDeleteEmptyParentZNodes() throws Exception {
        // try to delete the parent znodes if they are empty
        String remainingPath = getParentPath(getNormalizedPath(client.getNamespace()));
        final CuratorFramework nonNamespaceClient = client.usingNamespace(null);

        while (!isRootPath(remainingPath)) {
            try {
                nonNamespaceClient.delete().forPath(remainingPath);
            } catch (KeeperException.NotEmptyException ignored) {
                // We can only delete empty znodes
                break;
            }

            remainingPath = getParentPath(remainingPath);
        }
    }

    private static boolean isRootPath(String remainingPath) {
        return ZKPaths.PATH_SEPARATOR.equals(remainingPath);
    }

    @Nonnull
    private static String getNormalizedPath(String path) {
        return ZKPaths.makePath(path, "");
    }

    @Nonnull
    private static String getParentPath(String path) {
        return ZKPaths.getPathAndNode(path).getPath();
    }
}
