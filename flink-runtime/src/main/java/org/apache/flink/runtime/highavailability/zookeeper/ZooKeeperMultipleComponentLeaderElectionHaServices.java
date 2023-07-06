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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.DefaultMultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderelection.ZooKeeperMultipleComponentLeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ZooKeeper HA services that only use a single leader election per process.
 *
 * <pre>
 * /flink
 *      +/cluster_id_1/leader/latch
 *      |            |       /resource_manager/connection_info
 *      |            |       /dispatcher/connection_info
 *      |            |       /rest_server/connection_info
 *      |            |       /job-id-1/connection_info
 *      |            |       /job-id-2/connection_info
 *      |            |
 *      |            |
 *      |            +jobgraphs/job-id-1
 *      |            |         /job-id-2
 *      |            +jobs/job-id-1/checkpoints/latest
 *      |                 |                    /latest-1
 *      |                 |                    /latest-2
 *      |                 |       /checkpoint_id_counter
 * </pre>
 */
public class ZooKeeperMultipleComponentLeaderElectionHaServices extends AbstractHaServices {
    /** The curator resource to use. */
    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private final Object lock = new Object();

    private final FatalErrorHandler fatalErrorHandler;

    @Nullable
    @GuardedBy("lock")
    private MultipleComponentLeaderElectionService multipleComponentLeaderElectionService = null;

    public ZooKeeperMultipleComponentLeaderElectionHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration configuration,
            Executor executor,
            BlobStoreService blobStoreService,
            FatalErrorHandler fatalErrorHandler)
            throws IOException {
        super(
                configuration,
                executor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(configuration));
        this.curatorFrameworkWrapper = checkNotNull(curatorFrameworkWrapper);

        this.fatalErrorHandler = fatalErrorHandler;
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
    protected void internalClose() throws Exception {
        Exception exception = null;
        synchronized (lock) {
            if (multipleComponentLeaderElectionService != null) {
                try {
                    multipleComponentLeaderElectionService.close();
                } catch (Exception e) {
                    exception = e;
                }
                multipleComponentLeaderElectionService = null;
            }
        }

        try {
            curatorFrameworkWrapper.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        ExceptionUtils.tryRethrowException(exception);
    }

    @Override
    protected void internalCleanup() throws Exception {
        cleanupZooKeeperPaths();
    }

    @Override
    protected void internalCleanupJobData(JobID jobID) throws Exception {
        deleteZNode(ZooKeeperUtils.getLeaderPathForJob(jobID));
    }

    /** Cleans up leftover ZooKeeper paths. */
    private void cleanupZooKeeperPaths() throws Exception {
        deleteOwnedZNode();
        tryDeleteEmptyParentZNodes();
    }

    private void deleteOwnedZNode() throws Exception {
        deleteZNode("/");
    }

    protected void deleteZNode(String path) throws Exception {
        ZooKeeperUtils.deleteZNode(curatorFrameworkWrapper.asCuratorFramework(), path);
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

    private static String getNormalizedPath(String path) {
        return ZKPaths.makePath(path, "");
    }

    private static String getParentPath(String path) {
        return ZKPaths.getPathAndNode(path).getPath();
    }

    // ///////////////////////////////////////////////
    // LeaderElection/-Retrieval-related methods
    // ///////////////////////////////////////////////

    @Override
    protected MultipleComponentLeaderElectionDriverFactory createLeaderElectionDriverFactory(
            String leaderName) {
        return getOrInitializeSingleLeaderElectionService().createDriverFactory(leaderName);
    }

    private MultipleComponentLeaderElectionService getOrInitializeSingleLeaderElectionService() {
        synchronized (lock) {
            if (multipleComponentLeaderElectionService == null) {
                try {
                    multipleComponentLeaderElectionService =
                            new DefaultMultipleComponentLeaderElectionService(
                                    fatalErrorHandler,
                                    new ZooKeeperMultipleComponentLeaderElectionDriverFactory(
                                            ZooKeeperUtils.useNamespaceAndEnsurePath(
                                                    curatorFrameworkWrapper.asCuratorFramework(),
                                                    ZooKeeperUtils.getLeaderPath())));
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Could not initialize the %s",
                                    DefaultMultipleComponentLeaderElectionService.class
                                            .getSimpleName()),
                            e);
                }
            }

            return multipleComponentLeaderElectionService;
        }
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderPath) {
        // Maybe use a single service for leader retrieval
        return ZooKeeperUtils.createLeaderRetrievalService(
                curatorFrameworkWrapper.asCuratorFramework(),
                ZooKeeperUtils.generateZookeeperPath(ZooKeeperUtils.getLeaderPath(), leaderPath),
                configuration);
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return ZooKeeperUtils.getResourceManagerNode();
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return ZooKeeperUtils.getDispatcherNode();
    }

    @Override
    protected String getLeaderPathForJobManager(JobID jobID) {
        return jobID.toString();
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return ZooKeeperUtils.getRestServerNode();
    }
}
