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

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link AbstractHaServices} using Apache ZooKeeper. The services store
 * data in ZooKeeper's nodes as illustrated by the following tree structure:
 *
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/resource_manager/latch
 *      |            |                        /connection_info
 *      |            |       /dispatcher/latch
 *      |            |                  /connection_info
 *      |            |       /rest_server/latch
 *      |            |                   /connection_info
 *      |            |
 *      |            |
 *      |            +jobgraphs/job-id-1
 *      |            |         /job-id-2
 *      |            +jobs/job-id-1/leader/latch
 *      |                 |               /connection_info
 *      |                 |        /checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 *      |
 *      +/cluster_id_2/leader/resource_manager/latch
 *      |            |                        /connection_info
 *      |            |       /dispatcher/latch
 *      |            |                  /connection_info
 *      |            |       /rest_server/latch
 *      |            |                   /connection_info
 *      |            |
 *      |            +jobgraphs/job-id-2
 *      |            +jobs/job-id-2/leader/latch
 *      |                 |               /connection_info
 *      |                 |        /checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 * </pre>
 *
 * <p>The root path "/flink" is configurable via the option {@link
 * HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}. This makes sure Flink stores its data under specific
 * subtrees in ZooKeeper, for example to accommodate specific permission.
 *
 * <p>The "cluster_id" part identifies the data stored for a specific Flink "cluster". This
 * "cluster" can be either a standalone or containerized Flink cluster, or it can be job on a
 * framework like YARN (in a "per-job-cluster" mode).
 *
 * <p>In case of a "per-job-cluster" on YARN, the cluster-id is generated and configured
 * automatically by the client or dispatcher that submits the Job to YARN.
 *
 * <p>In the case of a standalone cluster, that cluster-id needs to be configured via {@link
 * HighAvailabilityOptions#HA_CLUSTER_ID}. All nodes with the same cluster id will join the same
 * cluster and participate in the execution of the same set of jobs.
 */
public class ZooKeeperHaServices extends AbstractHaServices {

    // ------------------------------------------------------------------------

    /** The curator resource to use. */
    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    public ZooKeeperHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Executor executor,
            Configuration configuration,
            BlobStoreService blobStoreService) {
        super(configuration, executor, blobStoreService);
        this.curatorFrameworkWrapper = checkNotNull(curatorFrameworkWrapper);
    }

    @Override
    public CheckpointRecoveryFactory createCheckpointRecoveryFactory() throws Exception {
        return new ZooKeeperCheckpointRecoveryFactory(
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        curatorFrameworkWrapper.asCuratorFramework(), ZooKeeperUtils.getJobsPath()),
                configuration,
                ioExecutor);
    }

    @Override
    public JobGraphStore createJobGraphStore() throws Exception {
        return ZooKeeperUtils.createJobGraphs(
                curatorFrameworkWrapper.asCuratorFramework(), configuration);
    }

    @Override
    public RunningJobsRegistry createRunningJobsRegistry() {
        return new ZooKeeperRunningJobsRegistry(
                curatorFrameworkWrapper.asCuratorFramework(), configuration);
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderPath) {
        return ZooKeeperUtils.createLeaderElectionService(
                curatorFrameworkWrapper.asCuratorFramework(), leaderPath);
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderPath) {
        return ZooKeeperUtils.createLeaderRetrievalService(
                curatorFrameworkWrapper.asCuratorFramework(), leaderPath, configuration);
    }

    @Override
    public void internalClose() {
        curatorFrameworkWrapper.close();
    }

    @Override
    public void internalCleanup() throws Exception {
        cleanupZooKeeperPaths();
    }

    @Override
    public void internalCleanupJobData(JobID jobID) throws Exception {
        deleteZNode(ZooKeeperUtils.getLeaderPathForJob(jobID));
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return ZooKeeperUtils.getLeaderPathForResourceManager();
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return ZooKeeperUtils.getLeaderPathForDispatcher();
    }

    @Override
    public String getLeaderPathForJobManager(final JobID jobID) {
        return ZooKeeperUtils.getLeaderPathForJobManager(jobID);
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return ZooKeeperUtils.getLeaderPathForRestServer();
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
            Stat stat = curatorFrameworkWrapper.asCuratorFramework().checkExists().forPath(path);
            if (stat == null) {
                logger.debug("znode {} has been deleted", path);
                return;
            }
            try {
                curatorFrameworkWrapper
                        .asCuratorFramework()
                        .delete()
                        .deletingChildrenIfNeeded()
                        .forPath(path);
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
        String remainingPath =
                getParentPath(
                        getNormalizedPath(
                                curatorFrameworkWrapper.asCuratorFramework().getNamespace()));
        final CuratorFramework nonNamespaceClient =
                curatorFrameworkWrapper.asCuratorFramework().usingNamespace(null);

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
