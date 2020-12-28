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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/** Tests for the {@link ZooKeeperJobGraphStoreWatcher}. */
public class ZooKeeperJobGraphStoreWatcherTest extends TestLogger {

    @Rule public ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);

    private Configuration configuration;

    private TestingJobGraphListener testingJobGraphListener;

    @Before
    public void setup() throws Exception {
        configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                temporaryFolder.newFolder().getAbsolutePath());
        testingJobGraphListener = new TestingJobGraphListener();
    }

    @Test
    public void testJobGraphAddedAndRemovedShouldNotifyGraphStoreListener() throws Exception {
        try (final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration)) {
            final JobGraphStoreWatcher jobGraphStoreWatcher =
                    createAndStartJobGraphStoreWatcher(client);

            final ZooKeeperStateHandleStore<JobGraph> stateHandleStore =
                    createStateHandleStore(client);

            final JobGraph jobGraph = new JobGraph();
            final JobID jobID = jobGraph.getJobID();
            stateHandleStore.addAndLock("/" + jobID, jobGraph);

            CommonTestUtils.waitUntilCondition(
                    () -> testingJobGraphListener.getAddedJobGraphs().size() > 0,
                    Deadline.fromNow(TIMEOUT));

            assertThat(testingJobGraphListener.getAddedJobGraphs(), contains(jobID));

            stateHandleStore.releaseAndTryRemove("/" + jobID);

            CommonTestUtils.waitUntilCondition(
                    () -> testingJobGraphListener.getRemovedJobGraphs().size() > 0,
                    Deadline.fromNow(TIMEOUT));
            assertThat(testingJobGraphListener.getRemovedJobGraphs(), contains(jobID));

            jobGraphStoreWatcher.stop();
        }
    }

    private JobGraphStoreWatcher createAndStartJobGraphStoreWatcher(CuratorFramework client)
            throws Exception {
        final ZooKeeperJobGraphStoreWatcher jobGraphStoreWatcher =
                new ZooKeeperJobGraphStoreWatcher(new PathChildrenCache(client, "/", false));
        jobGraphStoreWatcher.start(testingJobGraphListener);
        return jobGraphStoreWatcher;
    }

    private ZooKeeperStateHandleStore<JobGraph> createStateHandleStore(CuratorFramework client)
            throws Exception {
        final RetrievableStateStorageHelper<JobGraph> stateStorage =
                ZooKeeperUtils.createFileSystemStateStorage(configuration, "test_jobgraph");
        return new ZooKeeperStateHandleStore<>(client, stateStorage);
    }
}
