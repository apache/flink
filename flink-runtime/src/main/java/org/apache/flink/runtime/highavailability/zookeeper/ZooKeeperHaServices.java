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
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import java.io.IOException;
import java.util.concurrent.Executor;

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
 *
 * @deprecated in favour of {@link ZooKeeperMultipleComponentLeaderElectionHaServices}
 */
@Deprecated
public class ZooKeeperHaServices extends AbstractZooKeeperHaServices {

    // ------------------------------------------------------------------------

    public ZooKeeperHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Executor executor,
            Configuration configuration,
            BlobStoreService blobStoreService)
            throws IOException {
        super(curatorFrameworkWrapper, executor, configuration, blobStoreService);
    }

    @Override
    protected LeaderElectionService createLeaderElectionService(String leaderPath) {
        return ZooKeeperUtils.createLeaderElectionService(getCuratorFramework(), leaderPath);
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderPath) {
        return ZooKeeperUtils.createLeaderRetrievalService(
                getCuratorFramework(), leaderPath, configuration);
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
}
