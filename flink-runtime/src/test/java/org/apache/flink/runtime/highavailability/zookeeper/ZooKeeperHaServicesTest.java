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
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingContender;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.RetryNTimes;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link ZooKeeperHaServices}. */
public class ZooKeeperHaServicesTest extends TestLogger {

    @ClassRule public static final ZooKeeperResource ZOO_KEEPER_RESOURCE = new ZooKeeperResource();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    private static CuratorFramework client;

    @BeforeClass
    public static void setupClass() {
        client = startCuratorFramework();
        client.start();
    }

    @Before
    public void setup() throws Exception {
        final List<String> children = client.getChildren().forPath("/");

        for (String child : children) {
            if (!child.equals("zookeeper")) {
                client.delete().deletingChildrenIfNeeded().forPath('/' + child);
            }
        }
    }

    @AfterClass
    public static void teardownClass() {
        if (client != null) {
            client.close();
        }
    }

    /** Tests that a simple {@link ZooKeeperHaServices#close()} does not delete ZooKeeper paths. */
    @Test
    public void testSimpleClose() throws Exception {
        final String rootPath = "/foo/bar/flink";
        final Configuration configuration = createConfiguration(rootPath);

        final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

        runCleanupTest(configuration, blobStoreService, ZooKeeperHaServices::close);

        assertThat(blobStoreService.isClosed(), is(true));
        assertThat(blobStoreService.isClosedAndCleanedUpAllData(), is(false));

        final List<String> children = client.getChildren().forPath(rootPath);
        assertThat(children, is(not(empty())));
    }

    /**
     * Tests that the {@link ZooKeeperHaServices} cleans up all paths if it is closed via {@link
     * ZooKeeperHaServices#closeAndCleanupAllData()}.
     */
    @Test
    public void testSimpleCloseAndCleanupAllData() throws Exception {
        final Configuration configuration = createConfiguration("/foo/bar/flink");

        final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

        final List<String> initialChildren = client.getChildren().forPath("/");

        runCleanupTest(
                configuration, blobStoreService, ZooKeeperHaServices::closeAndCleanupAllData);

        assertThat(blobStoreService.isClosedAndCleanedUpAllData(), is(true));

        final List<String> children = client.getChildren().forPath("/");
        assertThat(children, is(equalTo(initialChildren)));
    }

    /** Tests that we can only delete the parent znodes as long as they are empty. */
    @Test
    public void testCloseAndCleanupAllDataWithUncle() throws Exception {
        final String prefix = "/foo/bar";
        final String flinkPath = prefix + "/flink";
        final Configuration configuration = createConfiguration(flinkPath);

        final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

        final String unclePath = prefix + "/foobar";
        client.create().creatingParentContainersIfNeeded().forPath(unclePath);

        runCleanupTest(
                configuration, blobStoreService, ZooKeeperHaServices::closeAndCleanupAllData);

        assertThat(blobStoreService.isClosedAndCleanedUpAllData(), is(true));

        assertThat(client.checkExists().forPath(flinkPath), is(nullValue()));
        assertThat(client.checkExists().forPath(unclePath), is(notNullValue()));
    }

    /** Tests that the ZooKeeperHaServices cleans up paths for job manager. */
    @Test
    public void testCleanupJobData() throws Exception {
        String rootPath = "/foo/bar/flink";
        final Configuration configuration = createConfiguration(rootPath);
        final String namespace = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);

        JobID jobID = new JobID();
        final String path = rootPath + namespace + ZooKeeperUtils.getJobsPath();

        final TestingBlobStoreService blobStoreService = new TestingBlobStoreService();

        runCleanupTestWithJob(
                configuration,
                blobStoreService,
                jobID,
                haServices -> {
                    final List<String> childrenBefore = client.getChildren().forPath(path);

                    haServices.cleanupJobData(jobID);

                    final List<String> childrenAfter = client.getChildren().forPath(path);

                    assertThat(childrenBefore, hasItem(jobID.toString()));
                    assertThat(childrenAfter, not(hasItem(jobID.toString())));
                });
    }

    private static CuratorFramework startCuratorFramework() {
        return CuratorFrameworkFactory.builder()
                .connectString(ZOO_KEEPER_RESOURCE.getConnectString())
                .retryPolicy(new RetryNTimes(50, 100))
                .build();
    }

    @Nonnull
    private Configuration createConfiguration(String rootPath) {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOO_KEEPER_RESOURCE.getConnectString());
        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, rootPath);
        return configuration;
    }

    private void runCleanupTest(
            Configuration configuration,
            TestingBlobStoreService blobStoreService,
            ThrowingConsumer<ZooKeeperHaServices, Exception> zooKeeperHaServicesConsumer)
            throws Exception {
        runCleanupTestWithJob(
                configuration, blobStoreService, new JobID(), zooKeeperHaServicesConsumer);
    }

    private void runCleanupTestWithJob(
            Configuration configuration,
            TestingBlobStoreService blobStoreService,
            JobID jobId,
            ThrowingConsumer<ZooKeeperHaServices, Exception> zooKeeperHaServicesConsumer)
            throws Exception {
        try (ZooKeeperHaServices zooKeeperHaServices =
                new ZooKeeperHaServices(
                        ZooKeeperUtils.startCuratorFramework(
                                configuration,
                                testingFatalErrorHandlerResource.getFatalErrorHandler()),
                        Executors.directExecutor(),
                        configuration,
                        blobStoreService)) {

            // create some Zk services to trigger the generation of paths
            final LeaderRetrievalService resourceManagerLeaderRetriever =
                    zooKeeperHaServices.getResourceManagerLeaderRetriever();
            final LeaderElectionService resourceManagerLeaderElectionService =
                    zooKeeperHaServices.getResourceManagerLeaderElectionService();

            final LeaderRetrievalService jobManagerLeaderRetriever =
                    zooKeeperHaServices.getJobManagerLeaderRetriever(jobId);
            final LeaderElectionService jobManagerLeaderElectionService =
                    zooKeeperHaServices.getJobManagerLeaderElectionService(jobId);

            final RunningJobsRegistry runningJobsRegistry =
                    zooKeeperHaServices.getRunningJobsRegistry();

            final LeaderRetrievalUtils.LeaderConnectionInfoListener resourceManagerLeaderListener =
                    new LeaderRetrievalUtils.LeaderConnectionInfoListener();
            resourceManagerLeaderElectionService.start(
                    new TestingContender(
                            "unused-resourcemanager-address",
                            resourceManagerLeaderElectionService));
            resourceManagerLeaderRetriever.start(resourceManagerLeaderListener);

            final LeaderRetrievalUtils.LeaderConnectionInfoListener jobManagerLeaderListener =
                    new LeaderRetrievalUtils.LeaderConnectionInfoListener();
            jobManagerLeaderElectionService.start(
                    new TestingContender(
                            "unused-jobmanager-address", jobManagerLeaderElectionService));
            jobManagerLeaderRetriever.start(jobManagerLeaderListener);

            runningJobsRegistry.setJobRunning(jobId);

            // Make sure that the respective zNodes have been properly created
            resourceManagerLeaderListener.getLeaderConnectionInfoFuture().join();
            jobManagerLeaderListener.getLeaderConnectionInfoFuture().join();

            resourceManagerLeaderRetriever.stop();
            resourceManagerLeaderElectionService.stop();
            jobManagerLeaderRetriever.stop();
            jobManagerLeaderElectionService.stop();
            runningJobsRegistry.clearJob(jobId);

            zooKeeperHaServicesConsumer.accept(zooKeeperHaServices);
        }
    }

    private static class TestingBlobStoreService implements BlobStoreService {

        private boolean closedAndCleanedUpAllData = false;
        private boolean closed = false;

        @Override
        public void closeAndCleanupAllData() {
            closedAndCleanedUpAllData = true;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        @Override
        public boolean put(File localFile, JobID jobId, BlobKey blobKey) {
            return false;
        }

        @Override
        public boolean delete(JobID jobId, BlobKey blobKey) {
            return false;
        }

        @Override
        public boolean deleteAll(JobID jobId) {
            return false;
        }

        @Override
        public boolean get(JobID jobId, BlobKey blobKey, File localFile) {
            return false;
        }

        private boolean isClosed() {
            return closed;
        }

        private boolean isClosedAndCleanedUpAllData() {
            return closedAndCleanedUpAllData;
        }
    }
}
