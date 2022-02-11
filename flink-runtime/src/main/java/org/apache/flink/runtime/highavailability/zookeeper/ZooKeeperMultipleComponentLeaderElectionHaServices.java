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
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.DefaultMultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionService;
import org.apache.flink.runtime.leaderelection.ZooKeeperMultipleComponentLeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.Executor;

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
public class ZooKeeperMultipleComponentLeaderElectionHaServices
        extends AbstractZooKeeperHaServices {

    private final Object lock = new Object();

    private final CuratorFramework leaderNamespacedCuratorFramework;

    private final FatalErrorHandler fatalErrorHandler;

    @Nullable
    @GuardedBy("lock")
    private MultipleComponentLeaderElectionService multipleComponentLeaderElectionService = null;

    public ZooKeeperMultipleComponentLeaderElectionHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration config,
            Executor ioExecutor,
            BlobStoreService blobStoreService,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        super(curatorFrameworkWrapper, ioExecutor, config, blobStoreService);
        this.leaderNamespacedCuratorFramework =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getCuratorFramework(), ZooKeeperUtils.getLeaderPath());
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderName) {
        return new DefaultLeaderElectionService(
                getOrInitializeSingleLeaderElectionService().createDriverFactory(leaderName));
    }

    private MultipleComponentLeaderElectionService getOrInitializeSingleLeaderElectionService() {
        synchronized (lock) {
            if (multipleComponentLeaderElectionService == null) {
                try {
                    multipleComponentLeaderElectionService =
                            new DefaultMultipleComponentLeaderElectionService(
                                    fatalErrorHandler,
                                    new ZooKeeperMultipleComponentLeaderElectionDriverFactory(
                                            leaderNamespacedCuratorFramework));
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
                leaderNamespacedCuratorFramework, leaderPath, configuration);
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
            super.internalClose();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        ExceptionUtils.tryRethrowException(exception);
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
