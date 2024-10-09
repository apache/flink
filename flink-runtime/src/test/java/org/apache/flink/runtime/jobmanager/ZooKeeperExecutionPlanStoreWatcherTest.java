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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ZooKeeperExecutionPlanStoreWatcher}. */
class ZooKeeperExecutionPlanStoreWatcherTest {

    @RegisterExtension
    public EachCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new EachCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir public Path temporaryFolder;

    private Configuration configuration;

    private TestingExecutionPlanListener testingExecutionPlanListener;

    @BeforeEach
    void setup() throws Exception {
        configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        testingExecutionPlanListener = new TestingExecutionPlanListener();
    }

    @Test
    void testJobGraphAddedAndRemovedShouldNotifyGraphStoreListener() throws Exception {
        try (final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration, NoOpFatalErrorHandler.INSTANCE)) {
            final CuratorFramework client = curatorFrameworkWrapper.asCuratorFramework();
            final ExecutionPlanStoreWatcher executionPlanStoreWatcher =
                    createAndStartExecutionPlanStoreWatcher(client);

            final ZooKeeperStateHandleStore<JobGraph> stateHandleStore =
                    createStateHandleStore(client);

            final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
            final JobID jobID = jobGraph.getJobID();
            stateHandleStore.addAndLock("/" + jobID, jobGraph);

            CommonTestUtils.waitUntilCondition(
                    () -> testingExecutionPlanListener.getAddedExecutionPlans().size() > 0);

            assertThat(testingExecutionPlanListener.getAddedExecutionPlans())
                    .containsExactly(jobID);

            stateHandleStore.releaseAndTryRemove("/" + jobID);

            CommonTestUtils.waitUntilCondition(
                    () -> testingExecutionPlanListener.getRemovedExecutionPlans().size() > 0);
            assertThat(testingExecutionPlanListener.getRemovedExecutionPlans())
                    .containsExactly(jobID);

            executionPlanStoreWatcher.stop();
        }
    }

    private ExecutionPlanStoreWatcher createAndStartExecutionPlanStoreWatcher(
            CuratorFramework client) throws Exception {
        final ZooKeeperExecutionPlanStoreWatcher executionPlanStoreWatcher =
                new ZooKeeperExecutionPlanStoreWatcher(new PathChildrenCache(client, "/", false));
        executionPlanStoreWatcher.start(testingExecutionPlanListener);
        return executionPlanStoreWatcher;
    }

    private ZooKeeperStateHandleStore<JobGraph> createStateHandleStore(CuratorFramework client)
            throws Exception {
        final RetrievableStateStorageHelper<JobGraph> stateStorage =
                ZooKeeperUtils.createFileSystemStateStorage(configuration, "test_jobgraph");
        return new ZooKeeperStateHandleStore<>(client, stateStorage);
    }
}
