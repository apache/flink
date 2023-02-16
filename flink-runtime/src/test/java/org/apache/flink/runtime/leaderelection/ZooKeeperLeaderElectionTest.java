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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.FlinkAssertions;
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
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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
 * use {@link TestingLeaderElectionEventHandler} which will not write the leader information to
 * ZooKeeper. For the complicated tests(e.g. multiple leaders), we will use {@link
 * DefaultLeaderElectionService} with {@link TestingContender}.
 */
class ZooKeeperLeaderElectionTest {

    @RegisterExtension
    private static final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private Configuration configuration;

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

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        LeaderElectionDriver leaderElectionDriver = null;
        LeaderRetrievalDriver leaderRetrievalDriver = null;
        try {

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            createZooKeeperClient(), electionEventHandler);
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(createZooKeeperClient())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            electionEventHandler.waitForLeader();
            final LeaderInformation confirmedLeaderInformation =
                    electionEventHandler.getConfirmedLeaderInformation();
            assertThat(confirmedLeaderInformation.getLeaderAddress()).isEqualTo(LEADER_ADDRESS);

            retrievalEventHandler.waitForNewLeader();

            assertThat(retrievalEventHandler.getLeaderSessionID())
                    .isEqualTo(confirmedLeaderInformation.getLeaderSessionID());
            assertThat(retrievalEventHandler.getAddress())
                    .isEqualTo(confirmedLeaderInformation.getLeaderAddress());
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }
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

        DefaultLeaderElectionService[] leaderElectionService =
                new DefaultLeaderElectionService[num];
        TestingContender[] contenders = new TestingContender[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        TestingListener listener = new TestingListener();

        try {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(createZooKeeperClient());

            LOG.debug("Start leader retrieval service for the TestingListener.");

            leaderRetrievalService.start(listener);

            for (int i = 0; i < num; i++) {
                leaderElectionService[i] =
                        ZooKeeperUtils.createLeaderElectionService(createZooKeeperClient());
                contenders[i] = new TestingContender(createAddress(i), leaderElectionService[i]);

                LOG.debug("Start leader election service for contender #{}.", i);

                leaderElectionService[i].start(contenders[i]);
            }

            String pattern = LEADER_ADDRESS + "_" + "(\\d+)";
            Pattern regex = Pattern.compile(pattern);

            int numberSeenLeaders = 0;

            while (deadline.hasTimeLeft() && numberSeenLeaders < num) {
                LOG.debug("Wait for new leader #{}.", numberSeenLeaders);
                String address = listener.waitForNewLeader();

                Matcher m = regex.matcher(address);

                if (m.find()) {
                    int index = Integer.parseInt(m.group(1));

                    TestingContender contender = contenders[index];

                    // check that the retrieval service has retrieved the correct leader
                    if (address.equals(createAddress(index))
                            && listener.getLeaderSessionID()
                                    .equals(contender.getLeaderSessionID())) {
                        // kill the election service of the leader
                        LOG.debug(
                                "Stop leader election service of contender #{}.",
                                numberSeenLeaders);
                        leaderElectionService[index].stop();
                        leaderElectionService[index] = null;

                        numberSeenLeaders++;
                    }
                } else {
                    fail("Did not find the leader's index.");
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

            for (DefaultLeaderElectionService electionService : leaderElectionService) {
                if (electionService != null) {
                    electionService.stop();
                }
            }
        }
    }

    private String createAddress(int i) {
        return LEADER_ADDRESS + "_" + i;
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

        DefaultLeaderElectionService[] leaderElectionService =
                new DefaultLeaderElectionService[num];
        TestingContender[] contenders = new TestingContender[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        TestingListener listener = new TestingListener();

        try {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(createZooKeeperClient());

            leaderRetrievalService.start(listener);

            for (int i = 0; i < num; i++) {
                leaderElectionService[i] =
                        ZooKeeperUtils.createLeaderElectionService(createZooKeeperClient());
                contenders[i] =
                        new TestingContender(
                                LEADER_ADDRESS + "_" + i + "_0", leaderElectionService[i]);

                leaderElectionService[i].start(contenders[i]);
            }

            String pattern = LEADER_ADDRESS + "_" + "(\\d+)" + "_" + "(\\d+)";
            Pattern regex = Pattern.compile(pattern);

            for (int i = 0; i < numTries; i++) {
                listener.waitForNewLeader();

                String address = listener.getAddress();

                Matcher m = regex.matcher(address);

                if (m.find()) {
                    int index = Integer.parseInt(m.group(1));
                    int lastTry = Integer.parseInt(m.group(2));

                    assertThat(listener.getLeaderSessionID())
                            .isEqualTo(contenders[index].getLeaderSessionID());

                    // stop leader election service = revoke leadership
                    leaderElectionService[index].stop();
                    // create new leader election service which takes part in the leader election
                    leaderElectionService[index] =
                            ZooKeeperUtils.createLeaderElectionService(createZooKeeperClient());
                    contenders[index] =
                            new TestingContender(
                                    LEADER_ADDRESS + "_" + index + "_" + (lastTry + 1),
                                    leaderElectionService[index]);

                    leaderElectionService[index].start(contenders[index]);
                } else {
                    throw new Exception("Did not find the leader's index.");
                }
            }

        } finally {
            if (leaderRetrievalService != null) {
                leaderRetrievalService.stop();
            }

            for (DefaultLeaderElectionService electionService : leaderElectionService) {
                if (electionService != null) {
                    electionService.stop();
                }
            }
        }
    }

    /** Tests that the leader update information will not be notified repeatedly. */
    @Test
    public void testLeaderChangeWriteLeaderInformationOnlyOnce() throws Exception {
        final LeaderInformationConsumer leaderInformationConsumer = new LeaderInformationConsumer();
        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS, leaderInformationConsumer);

        @SuppressWarnings("deprecation")
        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        try {
            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            createZooKeeperClient(), electionEventHandler);

            electionEventHandler.waitForLeader();
            final LeaderInformation confirmedLeaderInformation =
                    electionEventHandler.getConfirmedLeaderInformation();
            assertThat(confirmedLeaderInformation.getLeaderAddress()).isEqualTo(LEADER_ADDRESS);

            // First update will successfully complete.
            assertThat(leaderInformationConsumer.getFirstUpdateFuture())
                    .succeedsWithin(5, TimeUnit.SECONDS);
            // Wait for a while to make sure other updates don't appear.
            assertThat(leaderInformationConsumer.getAnotherUpdateFuture())
                    .withFailMessage("Another leader information update is not expected.")
                    .failsWithin(5, TimeUnit.MILLISECONDS)
                    .withThrowableOfType(TimeoutException.class);
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
        }
    }

    /**
     * Tests that the current leader is notified when his leader connection information in ZooKeeper
     * are overwritten. The leader must re-establish the correct leader connection information in
     * ZooKeeper.
     */
    @Test
    void testLeaderShouldBeCorrectedWhenOverwritten() throws Exception {
        final String faultyContenderUrl = "faultyContender";

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();

        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        LeaderRetrievalDriver leaderRetrievalDriver = null;

        CuratorFrameworkWithUnhandledErrorListener anotherCuratorFrameworkWrapper = null;

        try {

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            createZooKeeperClient(), electionEventHandler);

            electionEventHandler.waitForLeader();
            final LeaderInformation confirmedLeaderInformation =
                    electionEventHandler.getConfirmedLeaderInformation();
            assertThat(confirmedLeaderInformation.getLeaderAddress()).isEqualTo(LEADER_ADDRESS);

            anotherCuratorFrameworkWrapper =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration, NoOpFatalErrorHandler.INSTANCE);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeUTF(faultyContenderUrl);
            oos.writeObject(UUID.randomUUID());

            oos.close();

            // overwrite the current leader address, the leader should notice that
            boolean dataWritten = false;

            final String connectionInformationPath =
                    leaderElectionDriver.getConnectionInformationPath();

            while (!dataWritten) {
                anotherCuratorFrameworkWrapper
                        .asCuratorFramework()
                        .delete()
                        .forPath(connectionInformationPath);

                try {
                    anotherCuratorFrameworkWrapper
                            .asCuratorFramework()
                            .create()
                            .forPath(connectionInformationPath, baos.toByteArray());

                    dataWritten = true;
                } catch (KeeperException.NodeExistsException e) {
                    // this can happen if the leader election service was faster
                }
            }

            // The faulty leader should be corrected on ZooKeeper
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(createZooKeeperClient())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            if (retrievalEventHandler.waitForNewLeader().equals(faultyContenderUrl)) {
                retrievalEventHandler.waitForNewLeader();
            }

            assertThat(retrievalEventHandler.getLeaderSessionID())
                    .isEqualTo(confirmedLeaderInformation.getLeaderSessionID());
            assertThat(retrievalEventHandler.getAddress())
                    .isEqualTo(confirmedLeaderInformation.getLeaderAddress());
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }
            if (anotherCuratorFrameworkWrapper != null) {
                anotherCuratorFrameworkWrapper.close();
            }
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
        final String exMsg = "Test exception";
        final Exception testException = new Exception(exMsg);
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

            TestingLeaderElectionEventHandler electionEventHandler = null;
            LeaderElectionDriver leaderElectionDriver = null;
            try {
                electionEventHandler = new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
                leaderElectionDriver =
                        createAndInitLeaderElectionDriver(client, electionEventHandler);

                electionEventHandler.waitForError();

                assertThat(electionEventHandler.getError())
                        .isNotNull()
                        .satisfies(FlinkAssertions.anyCauseMatches(exMsg));
            } finally {
                if (leaderElectionDriver != null) {
                    leaderElectionDriver.close();
                }

                if (electionEventHandler != null) {
                    electionEventHandler.close();
                }
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
        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
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
                                    curatorFrameworkWrapper2.asCuratorFramework())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            cache =
                    CuratorCache.build(
                            curatorFrameworkWrapper2.asCuratorFramework(),
                            leaderElectionDriver.getConnectionInformationPath());

            final ExistsCacheListener existsListener =
                    ExistsCacheListener.createWithNodeIsMissingValidation(
                            cache, leaderElectionDriver.getConnectionInformationPath());
            cache.listenable().addListener(existsListener);
            cache.start();

            electionEventHandler.waitForLeader();

            retrievalEventHandler.waitForNewLeader();

            Future<Boolean> existsFuture = existsListener.nodeExists();

            existsFuture.get(timeout, TimeUnit.MILLISECONDS);

            final DeletedCacheListener deletedCacheListener =
                    DeletedCacheListener.createWithNodeExistValidation(
                            cache, leaderElectionDriver.getConnectionInformationPath());
            cache.listenable().addListener(deletedCacheListener);

            leaderElectionDriver.close();

            // now stop the underlying client
            curatorFrameworkWrapper.close();

            Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();

            // make sure that the leader node has been deleted
            deletedFuture.get(timeout, TimeUnit.MILLISECONDS);

            retrievalEventHandler.waitForEmptyLeaderInformation();
        } finally {
            electionEventHandler.close();
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }

            if (cache != null) {
                cache.close();
            }

            if (curatorFrameworkWrapper2 != null) {
                curatorFrameworkWrapper2.close();
            }
        }
    }

    @Test
    void testNotLeaderShouldNotCleanUpTheLeaderInformation() throws Exception {

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        ZooKeeperLeaderRetrievalDriver leaderRetrievalDriver = null;

        try {
            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            createZooKeeperClient(), electionEventHandler);

            electionEventHandler.waitForLeader();
            final LeaderInformation confirmedLeaderInformation =
                    electionEventHandler.getConfirmedLeaderInformation();
            assertThat(confirmedLeaderInformation.getLeaderAddress()).isEqualTo(LEADER_ADDRESS);

            // Leader is revoked
            leaderElectionDriver.notLeader();
            electionEventHandler.waitForRevokeLeader();
            assertThat(electionEventHandler.getConfirmedLeaderInformation())
                    .isEqualTo(LeaderInformation.empty());
            // The data on ZooKeeper has not been cleared
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(createZooKeeperClient())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            retrievalEventHandler.waitForNewLeader();

            assertThat(retrievalEventHandler.getLeaderSessionID())
                    .isEqualTo(confirmedLeaderInformation.getLeaderSessionID());
            assertThat(retrievalEventHandler.getAddress())
                    .isEqualTo(confirmedLeaderInformation.getLeaderAddress());
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }
        }
    }

    /**
     * Test that background errors in the {@link LeaderElectionDriver} are correctly forwarded to
     * the {@link FatalErrorHandler}.
     */
    @Test
    public void testUnExpectedErrorForwarding() throws Exception {
        LeaderElectionDriver leaderElectionDriver = null;
        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);

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
            electionEventHandler.close();

            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
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
            CuratorFramework client, TestingLeaderElectionEventHandler electionEventHandler)
            throws Exception {

        final ZooKeeperLeaderElectionDriver leaderElectionDriver =
                ZooKeeperUtils.createLeaderElectionDriverFactory(client)
                        .createLeaderElectionDriver(
                                electionEventHandler,
                                electionEventHandler::handleError,
                                LEADER_ADDRESS);
        electionEventHandler.init(leaderElectionDriver);
        return leaderElectionDriver;
    }

    private static class LeaderInformationConsumer implements Consumer<LeaderInformation> {

        final CompletableFuture<Void> firstUpdateFuture = new CompletableFuture<>();

        final CompletableFuture<Void> anotherUpdateFuture = new CompletableFuture<>();

        @Override
        public void accept(LeaderInformation leaderInformation) {
            if (!firstUpdateFuture.isDone()) {
                firstUpdateFuture.complete(null);
            } else {
                anotherUpdateFuture.complete(null);
            }
        }

        public CompletableFuture<Void> getFirstUpdateFuture() {
            return firstUpdateFuture;
        }

        public CompletableFuture<Void> getAnotherUpdateFuture() {
            return anotherUpdateFuture;
        }
    }
}
