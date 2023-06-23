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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.CreateBuilder;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ZooKeeperLeaderElectionDriver} and the {@link
 * org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver}. To directly test the
 * {@link ZooKeeperLeaderElectionDriver} and {@link
 * org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver}, some simple tests will
 * use {@link TestingLeaderElectionListener} which will not write the leader information to
 * ZooKeeper. For the complicated tests(e.g. multiple leaders), we will use {@link
 * DefaultLeaderElectionService} with {@link TestingGenericLeaderContender}.
 */
class ZooKeeperLeaderElectionTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private Configuration configuration;

    private static final String CONTENDER_ID = "contender-id";
    private static final String LEADER_ADDRESS = "akka//user/jobmanager";
    private static final long timeout = 200L * 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionTest.class);

    @BeforeEach
    void before() {
        configuration = new Configuration();

        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperResource.getCustomExtension().getConnectString());
        configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
    }

    /** Tests that the ZooKeeperLeaderElection/RetrievalService return both the correct URL. */
    @Test
    void testZooKeeperLeaderElectionRetrieval() throws Exception {
        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        try (LeaderElectionDriver leaderElectionDriver =
                        createAndInitLeaderElectionDriver(
                                createZooKeeperClient(), electionEventHandler);
                LeaderRetrievalDriver leaderRetrievalDriver =
                        ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                        createZooKeeperClient(), CONTENDER_ID)
                                .createLeaderRetrievalDriver(
                                        retrievalEventHandler,
                                        retrievalEventHandler::handleError)) {

            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

            final UUID leaderSessionID = UUID.randomUUID();
            leaderElectionDriver.publishLeaderInformation(
                    CONTENDER_ID, LeaderInformation.known(leaderSessionID, LEADER_ADDRESS));

            retrievalEventHandler.waitForNewLeader();

            assertThat(retrievalEventHandler.getLeaderSessionID()).isEqualTo(leaderSessionID);
            assertThat(retrievalEventHandler.getAddress()).isEqualTo(LEADER_ADDRESS);
        } finally {
            electionEventHandler.failIfErrorEventHappened();
        }
    }

    /**
     * Tests repeatedly the reelection of still available LeaderContender. After a contender has
     * been elected as the leader, it is removed. This forces the DefaultLeaderElectionService to
     * elect a new leader.
     */
    @Test
    void testZooKeeperReelection() throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5L));

        int num = 10;

        final ZooKeeperClientContext[] zooKeeperClientContext = new ZooKeeperClientContext[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        final String contenderID = "contender_id";
        final TestingListener listener = new TestingListener();
        try (final AutoCloseableRegistry closeables = new AutoCloseableRegistry()) {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(
                            createZooKeeperClient(), contenderID, new Configuration());

            LOG.debug("Start leader retrieval service for the TestingListener.");

            leaderRetrievalService.start(listener);

            for (int index = 0; index < num; index++) {
                zooKeeperClientContext[index] =
                        ZooKeeperClientContext.create(
                                index, 0, contenderID, createZooKeeperClient());
                closeables.registerCloseable(zooKeeperClientContext[index]);
            }

            int numberSeenLeaders = 0;
            while (deadline.hasTimeLeft() && numberSeenLeaders < num) {
                LOG.debug("Wait for new leader #{}.", numberSeenLeaders);

                final String address = listener.waitForNewLeader();
                int index = ZooKeeperClientContext.getIndexFromAddress(address);

                // check that the retrieval service has retrieved the correct leader
                if (listener.getLeaderSessionID()
                        .equals(zooKeeperClientContext[index].getLeaderSessionIDFromNextEvent())) {
                    // kill the election service of the leader
                    LOG.debug("Stop leader election service of contender #{}.", numberSeenLeaders);
                    zooKeeperClientContext[index].close();

                    numberSeenLeaders++;
                }
            }

            assertThat(deadline.isOverdue())
                    .as("Did not complete the leader reelection in time.")
                    .isFalse();
            assertThat(num).isEqualTo(numberSeenLeaders);
        } finally {
            if (leaderRetrievalService != null) {
                leaderRetrievalService.stop();
            }
        }
    }

    /**
     * Tests the repeated reelection of {@link LeaderContender} once the current leader dies.
     * Furthermore, it tests that new LeaderElectionServices can be started later on and that they
     * successfully register at ZooKeeper and take part in the leader election.
     */
    @Test
    void testZooKeeperReelectionWithReplacement() throws Exception {
        int num = 3;
        int numTries = 30;

        final ZooKeeperClientContext[] zooKeeperClientContext = new ZooKeeperClientContext[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        final String contenderID = "contender-id";
        final TestingListener listener = new TestingListener();
        try {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(
                            createZooKeeperClient(), contenderID, new Configuration());

            leaderRetrievalService.start(listener);

            for (int index = 0; index < num; index++) {
                zooKeeperClientContext[index] =
                        ZooKeeperClientContext.create(
                                index, 0, contenderID, createZooKeeperClient());
            }

            for (int run = 0; run < numTries; run++) {
                listener.waitForNewLeader();

                final String address = listener.getAddress();
                int index = ZooKeeperClientContext.getIndexFromAddress(address);
                int lastTry = ZooKeeperClientContext.getTryCountFromAddress(address);

                assertThat(listener.getLeaderSessionID())
                        .isEqualTo(zooKeeperClientContext[index].getLeaderSessionIDFromNextEvent());

                // stop leader election service = revoke leadership
                zooKeeperClientContext[index].close();

                // create new leader election service which takes part in the leader election
                zooKeeperClientContext[index] =
                        ZooKeeperClientContext.create(
                                index, lastTry + 1, contenderID, createZooKeeperClient());
            }
        } finally {
            if (leaderRetrievalService != null) {
                leaderRetrievalService.stop();
            }

            for (ZooKeeperClientContext ctx : zooKeeperClientContext) {
                ctx.close();
            }
        }
    }

    private static class ZooKeeperClientContext implements AutoCloseable {
        private final DefaultLeaderElectionService leaderElectionService;
        private final LeaderElection leaderElection;
        private final TestingFatalErrorHandler errorHandler = new TestingFatalErrorHandler();

        private final Queue<LeaderElectionEvent> eventQueue = new ConcurrentLinkedQueue<>();

        private static final String ADDRESS_FORMAT = "base-address_%s_%s";
        private static final Pattern ADDRESS_PATTERN =
                Pattern.compile(String.format(ADDRESS_FORMAT, "(\\d+)", "(\\d+)"));

        private static int getIndexFromAddress(String address) {
            return getValueFromAddress(address, 1);
        }

        private static int getTryCountFromAddress(String address) {
            return getValueFromAddress(address, 2);
        }

        private static int getValueFromAddress(String address, int groupIdx) {
            final Matcher matcher = ADDRESS_PATTERN.matcher(address);
            Preconditions.checkArgument(
                    matcher.find(),
                    "The passed address '"
                            + address
                            + "' does not match the expected pattern '"
                            + ADDRESS_FORMAT
                            + "'.");
            return Integer.parseInt(matcher.group(groupIdx));
        }

        private static ZooKeeperClientContext create(
                int contextID, int iteration, String contenderID, CuratorFramework client)
                throws Exception {
            return new ZooKeeperClientContext(
                    contenderID, String.format(ADDRESS_FORMAT, contextID, iteration), client);
        }

        private ZooKeeperClientContext(String contenderID, String address, CuratorFramework client)
                throws Exception {
            final LeaderElectionDriverFactory driverFactory =
                    new ZooKeeperLeaderElectionDriverFactory(client);

            leaderElectionService = new DefaultLeaderElectionService(driverFactory);
            leaderElectionService.startLeaderElectionBackend();

            leaderElection = leaderElectionService.createLeaderElection(contenderID);

            final LeaderContender contender =
                    TestingGenericLeaderContender.newBuilder(
                                    eventQueue, leaderElection, address, errorHandler::onFatalError)
                            .build();
            leaderElection.startLeaderElection(contender);
        }

        private UUID getLeaderSessionIDFromNextEvent() {
            return eventQueue.remove().asIsLeaderEvent().getLeaderSessionID();
        }

        @Override
        public void close() throws Exception {
            if (leaderElection != null) {
                leaderElection.close();
            }

            if (leaderElectionService != null) {
                leaderElectionService.close();
            }

            errorHandler.rethrowError();
        }
    }

    /** Tests that the leader update information will not be notified repeatedly. */
    @Test
    void testLeaderChangeWriteLeaderInformationOnlyOnce() throws Exception {
        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();

        try (ZooKeeperLeaderElectionDriver leaderElectionDriver =
                createAndInitLeaderElectionDriver(createZooKeeperClient(), electionEventHandler)) {

            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

            leaderElectionDriver.publishLeaderInformation(
                    CONTENDER_ID, LeaderInformation.known(UUID.randomUUID(), LEADER_ADDRESS));

            // First update will successfully complete.
            electionEventHandler.await(LeaderElectionEvent.LeaderInformationChangeEvent.class);

            // Wait for a while to make sure other updates don't appear.
            assertThat(
                            electionEventHandler.await(
                                    LeaderElectionEvent.LeaderInformationChangeEvent.class,
                                    Duration.ofMillis(5)))
                    .as("Another leader information update is not expected.")
                    .isEmpty();
        } finally {
            electionEventHandler.failIfErrorEventHappened();
        }
    }

    /**
     * Test that errors in the {@link LeaderElectionDriver} are correctly forwarded to the {@link
     * LeaderContender}.
     */
    @Test
    void testExceptionForwarding() throws Exception {
        final CreateBuilder mockCreateBuilder =
                mock(CreateBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        final Exception testException = new Exception("Test exception");
        try (final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration, NoOpFatalErrorHandler.INSTANCE)) {
            CuratorFramework client = spy(curatorFrameworkWrapper.asCuratorFramework());

            doAnswer(invocation -> mockCreateBuilder).when(client).create();

            when(mockCreateBuilder
                            .creatingParentsIfNeeded()
                            .withMode(ArgumentMatchers.any(CreateMode.class))
                            .forPath(anyString(), any(byte[].class)))
                    .thenThrow(testException);

            final TestingLeaderElectionListener electionEventHandler =
                    new TestingLeaderElectionListener();
            try (ZooKeeperLeaderElectionDriver leaderElectionDriver =
                    createAndInitLeaderElectionDriver(client, electionEventHandler)) {

                electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

                leaderElectionDriver.publishLeaderInformation(
                        CONTENDER_ID, LeaderInformation.known(UUID.randomUUID(), "some-address"));

                final LeaderElectionEvent.ErrorEvent errorEvent =
                        electionEventHandler.await(LeaderElectionEvent.ErrorEvent.class);
                assertThat(errorEvent.getError()).isEqualTo(testException);
            } finally {
                electionEventHandler.failIfErrorEventHappened();
            }
        }
    }

    /**
     * Tests that there is no information left in the ZooKeeper cluster after the ZooKeeper client
     * has terminated. In other words, checks that the ZooKeeperLeaderElection service uses
     * ephemeral nodes.
     */
    @Test
    void testEphemeralZooKeeperNodes() throws Exception {
        ZooKeeperLeaderElectionDriver leaderElectionDriver;
        LeaderRetrievalDriver leaderRetrievalDriver = null;
        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();

        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;
        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper2 = null;
        CuratorCache cache = null;

        try {
            curatorFrameworkWrapper =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration,
                            testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
            curatorFrameworkWrapper2 =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration,
                            testingFatalErrorHandlerResource.getTestingFatalErrorHandler());

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            curatorFrameworkWrapper.asCuratorFramework(), electionEventHandler);
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    curatorFrameworkWrapper2.asCuratorFramework(), CONTENDER_ID)
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            cache =
                    CuratorCache.build(
                            curatorFrameworkWrapper2.asCuratorFramework(),
                            ZooKeeperUtils.generateConnectionInformationPath(CONTENDER_ID));

            final ExistsCacheListener existsListener =
                    ExistsCacheListener.createWithNodeIsMissingValidation(
                            cache, ZooKeeperUtils.generateConnectionInformationPath(CONTENDER_ID));
            cache.listenable().addListener(existsListener);
            cache.start();

            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

            leaderElectionDriver.publishLeaderInformation(
                    CONTENDER_ID, LeaderInformation.known(UUID.randomUUID(), LEADER_ADDRESS));

            retrievalEventHandler.waitForNewLeader();

            Future<Boolean> existsFuture = existsListener.nodeExists();

            existsFuture.get(timeout, TimeUnit.MILLISECONDS);

            final DeletedCacheListener deletedCacheListener =
                    DeletedCacheListener.createWithNodeExistValidation(
                            cache, ZooKeeperUtils.generateConnectionInformationPath(CONTENDER_ID));
            cache.listenable().addListener(deletedCacheListener);

            leaderElectionDriver.close();

            // now stop the underlying client
            curatorFrameworkWrapper.close();

            Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();

            // make sure that the leader node has been deleted
            deletedFuture.get(timeout, TimeUnit.MILLISECONDS);

            retrievalEventHandler.waitForEmptyLeaderInformation();
        } finally {
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }

            if (cache != null) {
                cache.close();
            }

            if (curatorFrameworkWrapper2 != null) {
                curatorFrameworkWrapper2.close();
            }

            electionEventHandler.failIfErrorEventHappened();
        }
    }

    @Test
    void testNotLeaderShouldNotCleanUpTheLeaderInformation() throws Exception {
        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        try (ZooKeeperLeaderElectionDriver leaderElectionDriver =
                createAndInitLeaderElectionDriver(createZooKeeperClient(), electionEventHandler)) {

            // this call shouldn't block
            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

            final UUID leaderSessionID = UUID.randomUUID();
            leaderElectionDriver.publishLeaderInformation(
                    CONTENDER_ID, LeaderInformation.known(leaderSessionID, LEADER_ADDRESS));

            // Leader is revoked
            leaderElectionDriver.notLeader();
            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);

            // The data on ZooKeeper has not been cleared
            try (ZooKeeperLeaderRetrievalDriver leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    createZooKeeperClient(), CONTENDER_ID)
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError)) {

                retrievalEventHandler.waitForNewLeader();

                assertThat(retrievalEventHandler.getLeaderSessionID()).isEqualTo(leaderSessionID);
                assertThat(retrievalEventHandler.getAddress()).isEqualTo(LEADER_ADDRESS);
            }
        } finally {
            electionEventHandler.failIfErrorEventHappened();
        }
    }

    /**
     * Test that background errors in the {@link LeaderElectionDriver} are correctly forwarded to
     * the {@link FatalErrorHandler}.
     */
    @Test
    public void testUnExpectedErrorForwarding() throws Exception {
        LeaderElectionDriver leaderElectionDriver = null;
        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();

        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        final FlinkRuntimeException testException =
                new FlinkRuntimeException("testUnExpectedErrorForwarding");
        final CuratorFrameworkFactory.Builder curatorFrameworkBuilder =
                CuratorFrameworkFactory.builder()
                        .connectString(zooKeeperResource.getCustomExtension().getConnectString())
                        .retryPolicy(new ExponentialBackoffRetry(1, 0))
                        .aclProvider(
                                new ACLProvider() {
                                    // trigger background exception
                                    @Override
                                    public List<ACL> getDefaultAcl() {
                                        throw testException;
                                    }

                                    @Override
                                    public List<ACL> getAclForPath(String s) {
                                        throw testException;
                                    }
                                })
                        .namespace("flink");

        try (CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(curatorFrameworkBuilder, fatalErrorHandler)) {
            CuratorFramework clientWithErrorHandler = curatorFrameworkWrapper.asCuratorFramework();
            assertThat(fatalErrorHandler.getErrorFuture()).isNotDone();
            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(clientWithErrorHandler, electionEventHandler);
            assertThat(fatalErrorHandler.getErrorFuture().get()).isEqualTo(testException);
        } finally {
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }

            electionEventHandler.failIfErrorEventHappened();
        }
    }

    private CuratorFramework createZooKeeperClient() {
        return zooKeeperResource
                .getCustomExtension()
                .getZooKeeperClient(testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    private static class ExistsCacheListener implements CuratorCacheListener {

        final CompletableFuture<Boolean> existsPromise = new CompletableFuture<>();

        final CuratorCache cache;

        /**
         * Factory method that's used to ensure consistency in the implementation. The method
         * validates that the given node doesn't exist, yet.
         *
         * @throws IllegalStateException If the passed path is already present in the passed cache.
         */
        public static ExistsCacheListener createWithNodeIsMissingValidation(
                CuratorCache cache, String path) {
            Preconditions.checkState(
                    !cache.get(path).isPresent(),
                    "The given path %s should not lead to an already existing node. This listener will then check that the node was created.",
                    path);

            return new ExistsCacheListener(cache);
        }

        private ExistsCacheListener(final CuratorCache cache) {
            this.cache = cache;
        }

        public Future<Boolean> nodeExists() {
            return existsPromise;
        }

        @Override
        public void event(Type type, ChildData oldData, ChildData data) {
            if (type == Type.NODE_CREATED && data != null && !existsPromise.isDone()) {
                existsPromise.complete(true);
                cache.listenable().removeListener(this);
            }
        }
    }

    private static class DeletedCacheListener implements CuratorCacheListener {

        final CompletableFuture<Boolean> deletedPromise = new CompletableFuture<>();

        final CuratorCache cache;

        public static DeletedCacheListener createWithNodeExistValidation(
                CuratorCache cache, String path) {
            Preconditions.checkState(
                    cache.get(path).isPresent(),
                    "The given path %s should lead to an already existing node. This listener will then check that the node was properly deleted.",
                    path);
            return new DeletedCacheListener(cache);
        }

        private DeletedCacheListener(final CuratorCache cache) {
            this.cache = cache;
        }

        public Future<Boolean> nodeDeleted() {
            return deletedPromise;
        }

        @Override
        public void event(Type type, ChildData oldData, ChildData data) {
            if ((type == Type.NODE_DELETED || data == null) && !deletedPromise.isDone()) {
                deletedPromise.complete(true);
                cache.listenable().removeListener(this);
            }
        }
    }

    private ZooKeeperLeaderElectionDriver createAndInitLeaderElectionDriver(
            CuratorFramework client, TestingLeaderElectionListener electionEventHandler)
            throws Exception {

        return new ZooKeeperLeaderElectionDriverFactory(client).create(electionEventHandler);
    }
}
