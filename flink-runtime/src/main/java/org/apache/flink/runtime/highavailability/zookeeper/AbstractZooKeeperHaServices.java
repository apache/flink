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
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Abstract ZooKeeper based HA services. */
public abstract class AbstractZooKeeperHaServices extends AbstractHaServices {
    /** The curator resource to use. */
    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    public AbstractZooKeeperHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Executor executor,
            Configuration configuration,
            BlobStoreService blobStoreService)
            throws IOException {
        super(
                configuration,
                executor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(configuration));
        this.curatorFrameworkWrapper = checkNotNull(curatorFrameworkWrapper);
    }

    protected final CuratorFramework getCuratorFramework() {
        return curatorFrameworkWrapper.asCuratorFramework();
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
        curatorFrameworkWrapper.close();
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
}
