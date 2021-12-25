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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.CreateBuilder;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
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
public class ZooKeeperLeaderElectionTest extends TestLogger {
    private TestingServer testingServer;

    private Configuration configuration;

    private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private static final String TEST_URL = "akka//user/jobmanager";
    private static final LeaderInformation TEST_LEADER =
            LeaderInformation.known(UUID.randomUUID(), TEST_URL);
    private static final long timeout = 200L * 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionTest.class);

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    @Before
    public void before() {
        try {
            testingServer = new TestingServer();
        } catch (Exception e) {
            throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
        }

        configuration = new Configuration();

        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

        curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(
                        configuration, testingFatalErrorHandlerResource.getFatalErrorHandler());
    }

    @After
    public void after() throws IOException {
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
            curatorFrameworkWrapper = null;
        }

        if (testingServer != null) {
            testingServer.stop();
            testingServer = null;
        }
    }

    /** Tests that the ZooKeeperLeaderElection/RetrievalService return both the correct URL. */
    @Test
    public void testZooKeeperLeaderElectionRetrieval() throws Exception {

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(TEST_LEADER);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        LeaderElectionDriver leaderElectionDriver = null;
        LeaderRetrievalDriver leaderRetrievalDriver = null;
        try {

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            curatorFrameworkWrapper.asCuratorFramework(), electionEventHandler);
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    curatorFrameworkWrapper.asCuratorFramework())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            electionEventHandler.waitForLeader(timeout);
            assertThat(electionEventHandler.getConfirmedLeaderInformation(), is(TEST_LEADER));

            retrievalEventHandler.waitForNewLeader(timeout);

            assertThat(
                    retrievalEventHandler.getLeaderSessionID(),
                    is(TEST_LEADER.getLeaderSessionID()));
            assertThat(retrievalEventHandler.getAddress(), is(TEST_LEADER.getLeaderAddress()));
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
    public void testZooKeeperReelection() throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5L));

        int num = 10;

        DefaultLeaderElectionService[] leaderElectionService =
                new DefaultLeaderElectionService[num];
        TestingContender[] contenders = new TestingContender[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        TestingListener listener = new TestingListener();

        try {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(
                            curatorFrameworkWrapper.asCuratorFramework());

            LOG.debug("Start leader retrieval service for the TestingListener.");

            leaderRetrievalService.start(listener);

            for (int i = 0; i < num; i++) {
                leaderElectionService[i] =
                        ZooKeeperUtils.createLeaderElectionService(
                                curatorFrameworkWrapper.asCuratorFramework());
                contenders[i] = new TestingContender(createAddress(i), leaderElectionService[i]);

                LOG.debug("Start leader election service for contender #{}.", i);

                leaderElectionService[i].start(contenders[i]);
            }

            String pattern = TEST_URL + "_" + "(\\d+)";
            Pattern regex = Pattern.compile(pattern);

            int numberSeenLeaders = 0;

            while (deadline.hasTimeLeft() && numberSeenLeaders < num) {
                LOG.debug("Wait for new leader #{}.", numberSeenLeaders);
                String address = listener.waitForNewLeader(deadline.timeLeft().toMillis());

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

            assertFalse("Did not complete the leader reelection in time.", deadline.isOverdue());
            assertEquals(num, numberSeenLeaders);

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

    @Nonnull
    private String createAddress(int i) {
        return TEST_URL + "_" + i;
    }

    /**
     * Tests the repeated reelection of {@link LeaderContender} once the current leader dies.
     * Furthermore, it tests that new LeaderElectionServices can be started later on and that they
     * successfully register at ZooKeeper and take part in the leader election.
     */
    @Test
    public void testZooKeeperReelectionWithReplacement() throws Exception {
        int num = 3;
        int numTries = 30;

        DefaultLeaderElectionService[] leaderElectionService =
                new DefaultLeaderElectionService[num];
        TestingContender[] contenders = new TestingContender[num];
        DefaultLeaderRetrievalService leaderRetrievalService = null;

        TestingListener listener = new TestingListener();

        try {
            leaderRetrievalService =
                    ZooKeeperUtils.createLeaderRetrievalService(
                            curatorFrameworkWrapper.asCuratorFramework());

            leaderRetrievalService.start(listener);

            for (int i = 0; i < num; i++) {
                leaderElectionService[i] =
                        ZooKeeperUtils.createLeaderElectionService(
                                curatorFrameworkWrapper.asCuratorFramework());
                contenders[i] =
                        new TestingContender(TEST_URL + "_" + i + "_0", leaderElectionService[i]);

                leaderElectionService[i].start(contenders[i]);
            }

            String pattern = TEST_URL + "_" + "(\\d+)" + "_" + "(\\d+)";
            Pattern regex = Pattern.compile(pattern);

            for (int i = 0; i < numTries; i++) {
                listener.waitForNewLeader(timeout);

                String address = listener.getAddress();

                Matcher m = regex.matcher(address);

                if (m.find()) {
                    int index = Integer.parseInt(m.group(1));
                    int lastTry = Integer.parseInt(m.group(2));

                    assertEquals(
                            listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());

                    // stop leader election service = revoke leadership
                    leaderElectionService[index].stop();
                    // create new leader election service which takes part in the leader election
                    leaderElectionService[index] =
                            ZooKeeperUtils.createLeaderElectionService(
                                    curatorFrameworkWrapper.asCuratorFramework());
                    contenders[index] =
                            new TestingContender(
                                    TEST_URL + "_" + index + "_" + (lastTry + 1),
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

    /**
     * Tests that the current leader is notified when his leader connection information in ZooKeeper
     * are overwritten. The leader must re-establish the correct leader connection information in
     * ZooKeeper.
     */
    @Test
    public void testLeaderShouldBeCorrectedWhenOverwritten() throws Exception {
        final String faultyContenderUrl = "faultyContender";

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(TEST_LEADER);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();

        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        LeaderRetrievalDriver leaderRetrievalDriver = null;

        CuratorFrameworkWithUnhandledErrorListener anotherCuratorFrameworkWrapper = null;

        try {

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            curatorFrameworkWrapper.asCuratorFramework(), electionEventHandler);

            electionEventHandler.waitForLeader(timeout);
            assertThat(electionEventHandler.getConfirmedLeaderInformation(), is(TEST_LEADER));

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
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    curatorFrameworkWrapper.asCuratorFramework())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            if (retrievalEventHandler.waitForNewLeader(timeout).equals(faultyContenderUrl)) {
                retrievalEventHandler.waitForNewLeader(timeout);
            }

            assertThat(
                    retrievalEventHandler.getLeaderSessionID(),
                    is(TEST_LEADER.getLeaderSessionID()));
            assertThat(retrievalEventHandler.getAddress(), is(TEST_LEADER.getLeaderAddress()));
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
    public void testExceptionForwarding() throws Exception {
        LeaderElectionDriver leaderElectionDriver = null;
        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(TEST_LEADER);

        CuratorFramework client = null;
        final CreateBuilder mockCreateBuilder =
                mock(CreateBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        final String exMsg = "Test exception";
        final Exception testException = new Exception(exMsg);
        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);

        try {
            client = spy(curatorFrameworkWrapper.asCuratorFramework());

            doAnswer(invocation -> mockCreateBuilder).when(client).create();

            when(mockCreateBuilder
                            .creatingParentsIfNeeded()
                            .withMode(Matchers.any(CreateMode.class))
                            .forPath(anyString(), any(byte[].class)))
                    .thenThrow(testException);

            leaderElectionDriver = createAndInitLeaderElectionDriver(client, electionEventHandler);

            electionEventHandler.waitForError(timeout);

            assertNotNull(electionEventHandler.getError());
            assertThat(
                    ExceptionUtils.findThrowableWithMessage(electionEventHandler.getError(), exMsg)
                            .isPresent(),
                    is(true));
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }

            if (curatorFrameworkWrapper != null) {
                curatorFrameworkWrapper.close();
            }
        }
    }

    /**
     * Tests that there is no information left in the ZooKeeper cluster after the ZooKeeper client
     * has terminated. In other words, checks that the ZooKeeperLeaderElection service uses
     * ephemeral nodes.
     */
    @Test
    public void testEphemeralZooKeeperNodes() throws Exception {
        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        LeaderRetrievalDriver leaderRetrievalDriver = null;
        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(TEST_LEADER);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();

        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper = null;
        CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper2 = null;
        NodeCache cache = null;

        try {
            curatorFrameworkWrapper =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration, testingFatalErrorHandlerResource.getFatalErrorHandler());
            curatorFrameworkWrapper2 =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration, testingFatalErrorHandlerResource.getFatalErrorHandler());

            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            curatorFrameworkWrapper.asCuratorFramework(), electionEventHandler);
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    curatorFrameworkWrapper2.asCuratorFramework())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            cache =
                    new NodeCache(
                            curatorFrameworkWrapper2.asCuratorFramework(),
                            leaderElectionDriver.getConnectionInformationPath());

            ExistsCacheListener existsListener = new ExistsCacheListener(cache);
            DeletedCacheListener deletedCacheListener = new DeletedCacheListener(cache);

            cache.getListenable().addListener(existsListener);
            cache.start();

            electionEventHandler.waitForLeader(timeout);

            retrievalEventHandler.waitForNewLeader(timeout);

            Future<Boolean> existsFuture = existsListener.nodeExists();

            existsFuture.get(timeout, TimeUnit.MILLISECONDS);

            cache.getListenable().addListener(deletedCacheListener);

            leaderElectionDriver.close();

            // now stop the underlying client
            curatorFrameworkWrapper.close();

            Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();

            // make sure that the leader node has been deleted
            deletedFuture.get(timeout, TimeUnit.MILLISECONDS);

            try {
                retrievalEventHandler.waitForNewLeader(1000L);

                fail(
                        "TimeoutException was expected because there is no leader registered and "
                                + "thus there shouldn't be any leader information in ZooKeeper.");
            } catch (TimeoutException e) {
                // that was expected
            }
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
    public void testNotLeaderShouldNotCleanUpTheLeaderInformation() throws Exception {

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(TEST_LEADER);
        final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                new TestingLeaderRetrievalEventHandler();
        ZooKeeperLeaderElectionDriver leaderElectionDriver = null;
        ZooKeeperLeaderRetrievalDriver leaderRetrievalDriver = null;

        try {
            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(
                            curatorFrameworkWrapper.asCuratorFramework(), electionEventHandler);

            electionEventHandler.waitForLeader(timeout);
            assertThat(electionEventHandler.getConfirmedLeaderInformation(), is(TEST_LEADER));

            // Leader is revoked
            leaderElectionDriver.notLeader();
            electionEventHandler.waitForRevokeLeader(timeout);
            assertThat(
                    electionEventHandler.getConfirmedLeaderInformation(),
                    is(LeaderInformation.empty()));
            // The data on ZooKeeper it not be cleared
            leaderRetrievalDriver =
                    ZooKeeperUtils.createLeaderRetrievalDriverFactory(
                                    curatorFrameworkWrapper.asCuratorFramework())
                            .createLeaderRetrievalDriver(
                                    retrievalEventHandler, retrievalEventHandler::handleError);

            retrievalEventHandler.waitForNewLeader(timeout);

            assertThat(
                    retrievalEventHandler.getLeaderSessionID(),
                    is(TEST_LEADER.getLeaderSessionID()));
            assertThat(retrievalEventHandler.getAddress(), is(TEST_LEADER.getLeaderAddress()));
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
                new TestingLeaderElectionEventHandler(TEST_LEADER);

        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        final FlinkRuntimeException testException =
                new FlinkRuntimeException("testUnExpectedErrorForwarding");
        final CuratorFrameworkFactory.Builder curatorFrameworkBuilder =
                CuratorFrameworkFactory.builder()
                        .connectString(testingServer.getConnectString())
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
            assertFalse(fatalErrorHandler.getErrorFuture().isDone());
            leaderElectionDriver =
                    createAndInitLeaderElectionDriver(clientWithErrorHandler, electionEventHandler);
            assertThat(
                    fatalErrorHandler.getErrorFuture().join(),
                    FlinkMatchers.containsCause(testException));
        } finally {
            electionEventHandler.close();

            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
        }
    }

    private static class ExistsCacheListener implements NodeCacheListener {

        final CompletableFuture<Boolean> existsPromise = new CompletableFuture<>();

        final NodeCache cache;

        public ExistsCacheListener(final NodeCache cache) {
            this.cache = cache;
        }

        public Future<Boolean> nodeExists() {
            return existsPromise;
        }

        @Override
        public void nodeChanged() throws Exception {
            ChildData data = cache.getCurrentData();

            if (data != null && !existsPromise.isDone()) {
                existsPromise.complete(true);
                cache.getListenable().removeListener(this);
            }
        }
    }

    private static class DeletedCacheListener implements NodeCacheListener {

        final CompletableFuture<Boolean> deletedPromise = new CompletableFuture<>();

        final NodeCache cache;

        public DeletedCacheListener(final NodeCache cache) {
            this.cache = cache;
        }

        public Future<Boolean> nodeDeleted() {
            return deletedPromise;
        }

        @Override
        public void nodeChanged() throws Exception {
            ChildData data = cache.getCurrentData();

            if (data == null && !deletedPromise.isDone()) {
                deletedPromise.complete(true);
                cache.getListenable().removeListener(this);
            }
        }
    }

    private ZooKeeperLeaderElectionDriver createAndInitLeaderElectionDriver(
            CuratorFramework client, TestingLeaderElectionEventHandler electionEventHandler)
            throws Exception {

        final ZooKeeperLeaderElectionDriver leaderElectionDriver =
                ZooKeeperUtils.createLeaderElectionDriverFactory(client)
                        .createLeaderElectionDriver(
                                electionEventHandler, electionEventHandler::handleError, TEST_URL);
        electionEventHandler.init(leaderElectionDriver);
        return leaderElectionDriver;
    }
}
