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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link HeartbeatManager}. */
public class HeartbeatManagerTest extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManagerTest.class);
    public static final long HEARTBEAT_INTERVAL = 50L;
    public static final long HEARTBEAT_TIMEOUT = 200L;
    public static final int FAILED_RPC_THRESHOLD = -1;
    private static final Duration willNotCompleteWithin = Duration.ofMillis(20L);

    /**
     * Tests that regular heartbeat signal triggers the right callback functions in the {@link
     * HeartbeatListener}.
     */
    @Test
    public void testRegularHeartbeat() throws InterruptedException {
        final long heartbeatTimeout = 1000L;
        ResourceID ownResourceID = new ResourceID("foobar");
        ResourceID targetResourceID = new ResourceID("barfoo");
        final int outputPayload = 42;
        final ArrayBlockingQueue<String> reportedPayloads = new ArrayBlockingQueue<>(2);
        final TestingHeartbeatListener<String, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<String, Integer>()
                        .setReportPayloadConsumer(
                                (ignored, payload) -> reportedPayloads.offer(payload))
                        .setRetrievePayloadFunction((ignored) -> outputPayload)
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<String, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        ownResourceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final ArrayBlockingQueue<Integer> reportedPayloadsHeartbeatTarget =
                new ArrayBlockingQueue<>(2);
        final TestingHeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatFunction(
                                (ignoredA, payload) -> {
                                    reportedPayloadsHeartbeatTarget.offer(payload);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingHeartbeatTarget();

        heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

        final String inputPayload1 = "foobar";
        heartbeatManager.requestHeartbeat(targetResourceID, inputPayload1);

        assertThat(reportedPayloads.take(), is(inputPayload1));
        assertThat(reportedPayloadsHeartbeatTarget.take(), is(outputPayload));

        final String inputPayload2 = "barfoo";
        heartbeatManager.receiveHeartbeat(targetResourceID, inputPayload2);
        assertThat(reportedPayloads.take(), is(inputPayload2));
    }

    /** Tests that the heartbeat monitors are updated when receiving a new heartbeat signal. */
    @Test
    public void testHeartbeatMonitorUpdate() {
        long heartbeatTimeout = 1000L;
        ResourceID ownResourceID = new ResourceID("foobar");
        ResourceID targetResourceID = new ResourceID("barfoo");
        Object expectedObject = new Object();
        HeartbeatListener<Object, Object> heartbeatListener =
                new TestingHeartbeatListenerBuilder<>()
                        .setRetrievePayloadFunction(
                                ignored -> CompletableFuture.completedFuture(expectedObject))
                        .createNewTestingHeartbeatListener();
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        HeartbeatManagerImpl<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        ownResourceID,
                        heartbeatListener,
                        manuallyTriggeredScheduledExecutor,
                        LOG);

        heartbeatManager.monitorTarget(
                targetResourceID,
                new TestingHeartbeatTargetBuilder<>().createTestingHeartbeatTarget());

        heartbeatManager.receiveHeartbeat(targetResourceID, expectedObject);

        final List<ScheduledFuture<?>> scheduledTasksAfterHeartbeat =
                manuallyTriggeredScheduledExecutor.getAllScheduledTasks();

        assertThat(scheduledTasksAfterHeartbeat, hasSize(2));
        // the first scheduled future should be cancelled by the heartbeat update
        assertTrue(scheduledTasksAfterHeartbeat.get(0).isCancelled());
    }

    /** Tests that a heartbeat timeout is signaled if the heartbeat is not reported in time. */
    @Test
    public void testHeartbeatTimeout() throws Exception {
        int numHeartbeats = 6;
        final int payload = 42;

        ResourceID ownResourceID = new ResourceID("foobar");
        ResourceID targetResourceID = new ResourceID("barfoo");

        final CompletableFuture<ResourceID> timeoutFuture = new CompletableFuture<>();
        final TestingHeartbeatListener<Integer, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<Integer, Integer>()
                        .setRetrievePayloadFunction(ignored -> payload)
                        .setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<Integer, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        HEARTBEAT_TIMEOUT,
                        FAILED_RPC_THRESHOLD,
                        ownResourceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final HeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>().createTestingHeartbeatTarget();

        heartbeatManager.monitorTarget(targetResourceID, heartbeatTarget);

        for (int i = 0; i < numHeartbeats; i++) {
            heartbeatManager.receiveHeartbeat(targetResourceID, payload);
            Thread.sleep(HEARTBEAT_INTERVAL);
        }

        assertFalse(timeoutFuture.isDone());

        ResourceID timeoutResourceID =
                timeoutFuture.get(2 * HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);

        assertEquals(targetResourceID, timeoutResourceID);
    }

    /**
     * Tests the heartbeat interplay between the {@link HeartbeatManagerImpl} and the {@link
     * HeartbeatManagerSenderImpl}. The sender should regularly trigger heartbeat requests which are
     * fulfilled by the receiver. Upon stopping the receiver, the sender should notify the heartbeat
     * listener about the heartbeat timeout.
     *
     * @throws Exception
     */
    @Test
    public void testHeartbeatCluster() throws Exception {
        ResourceID resourceIdTarget = new ResourceID("foobar");
        ResourceID resourceIDSender = new ResourceID("barfoo");
        final int targetPayload = 42;
        final AtomicInteger numReportPayloadCallsTarget = new AtomicInteger(0);
        final TestingHeartbeatListener<String, Integer> heartbeatListenerTarget =
                new TestingHeartbeatListenerBuilder<String, Integer>()
                        .setRetrievePayloadFunction(ignored -> targetPayload)
                        .setReportPayloadConsumer(
                                (ignoredA, ignoredB) ->
                                        numReportPayloadCallsTarget.incrementAndGet())
                        .createNewTestingHeartbeatListener();

        final String senderPayload = "1337";
        final CompletableFuture<ResourceID> targetHeartbeatTimeoutFuture =
                new CompletableFuture<>();
        final AtomicInteger numReportPayloadCallsSender = new AtomicInteger(0);
        final TestingHeartbeatListener<Integer, String> heartbeatListenerSender =
                new TestingHeartbeatListenerBuilder<Integer, String>()
                        .setRetrievePayloadFunction(ignored -> senderPayload)
                        .setNotifyHeartbeatTimeoutConsumer(targetHeartbeatTimeoutFuture::complete)
                        .setReportPayloadConsumer(
                                (ignoredA, ignoredB) ->
                                        numReportPayloadCallsSender.incrementAndGet())
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<String, Integer> heartbeatManagerTarget =
                new HeartbeatManagerImpl<>(
                        HEARTBEAT_TIMEOUT,
                        FAILED_RPC_THRESHOLD,
                        resourceIdTarget,
                        heartbeatListenerTarget,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        HeartbeatManagerSenderImpl<Integer, String> heartbeatManagerSender =
                new HeartbeatManagerSenderImpl<>(
                        HEARTBEAT_INTERVAL,
                        HEARTBEAT_TIMEOUT,
                        FAILED_RPC_THRESHOLD,
                        resourceIDSender,
                        heartbeatListenerSender,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        heartbeatManagerTarget.monitorTarget(resourceIDSender, heartbeatManagerSender);
        heartbeatManagerSender.monitorTarget(resourceIdTarget, heartbeatManagerTarget);

        Thread.sleep(2 * HEARTBEAT_TIMEOUT);

        assertFalse(targetHeartbeatTimeoutFuture.isDone());

        heartbeatManagerTarget.stop();

        ResourceID timeoutResourceID =
                targetHeartbeatTimeoutFuture.get(2 * HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);

        assertThat(timeoutResourceID, is(resourceIdTarget));

        int numberHeartbeats = (int) (2 * HEARTBEAT_TIMEOUT / HEARTBEAT_INTERVAL);

        final Matcher<Integer> numberHeartbeatsMatcher = greaterThanOrEqualTo(numberHeartbeats / 2);
        assertThat(numReportPayloadCallsTarget.get(), is(numberHeartbeatsMatcher));
        assertThat(numReportPayloadCallsSender.get(), is(numberHeartbeatsMatcher));
    }

    /** Tests that after unmonitoring a target, there won't be a timeout triggered. */
    @Test
    public void testTargetUnmonitoring() throws Exception {
        // this might be too aggressive for Travis, let's see...
        long heartbeatTimeout = 50L;
        ResourceID resourceID = new ResourceID("foobar");
        ResourceID targetID = new ResourceID("target");
        final int payload = 42;

        final CompletableFuture<ResourceID> timeoutFuture = new CompletableFuture<>();
        final TestingHeartbeatListener<Integer, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<Integer, Integer>()
                        .setRetrievePayloadFunction(ignored -> payload)
                        .setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
                        .createNewTestingHeartbeatListener();

        HeartbeatManager<Integer, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        resourceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final HeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>().createTestingHeartbeatTarget();
        heartbeatManager.monitorTarget(targetID, heartbeatTarget);

        heartbeatManager.unmonitorTarget(targetID);

        try {
            timeoutFuture.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);
            fail("Timeout should time out.");
        } catch (TimeoutException ignored) {
            // the timeout should not be completed since we unmonitored the target
        }
    }

    /** Tests that the last heartbeat from an unregistered target equals -1. */
    @Test
    public void testLastHeartbeatFromUnregisteredTarget() {
        final long heartbeatTimeout = 100L;
        final ResourceID resourceId = ResourceID.generate();
        final HeartbeatListener<Object, Object> heartbeatListener =
                new TestingHeartbeatListenerBuilder<>().createNewTestingHeartbeatListener();

        HeartbeatManager<?, ?> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        resourceId,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            assertEquals(-1L, heartbeatManager.getLastHeartbeatFrom(ResourceID.generate()));
        } finally {
            heartbeatManager.stop();
        }
    }

    /** Tests that we can correctly retrieve the last heartbeat for registered targets. */
    @Test
    public void testLastHeartbeatFrom() {
        final long heartbeatTimeout = 100L;
        final ResourceID resourceId = ResourceID.generate();
        final ResourceID target = ResourceID.generate();

        HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        resourceId,
                        new TestingHeartbeatListenerBuilder<>().createNewTestingHeartbeatListener(),
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(
                    target, new TestingHeartbeatTargetBuilder<>().createTestingHeartbeatTarget());

            assertEquals(0L, heartbeatManager.getLastHeartbeatFrom(target));

            final long currentTime = System.currentTimeMillis();

            heartbeatManager.receiveHeartbeat(target, null);

            assertTrue(heartbeatManager.getLastHeartbeatFrom(target) >= currentTime);
        } finally {
            heartbeatManager.stop();
        }
    }

    /**
     * Tests that the heartbeat target {@link ResourceID} is properly passed to the {@link
     * HeartbeatListener} by the {@link HeartbeatManagerImpl}.
     */
    @Test
    public void testHeartbeatManagerTargetPayload() throws Exception {
        final long heartbeatTimeout = 100L;

        final ResourceID someTargetId = ResourceID.generate();
        final ResourceID specialTargetId = ResourceID.generate();

        final Map<ResourceID, Integer> payloads = new HashMap<>(2);
        payloads.put(someTargetId, 0);
        payloads.put(specialTargetId, 1);

        final CompletableFuture<Integer> someHeartbeatPayloadFuture = new CompletableFuture<>();
        final TestingHeartbeatTarget<Integer> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatFunction(
                                (ignored, payload) -> {
                                    someHeartbeatPayloadFuture.complete(payload);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingHeartbeatTarget();

        final CompletableFuture<Integer> specialHeartbeatPayloadFuture = new CompletableFuture<>();
        final TestingHeartbeatTarget<Integer> specialHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatFunction(
                                (ignored, payload) -> {
                                    specialHeartbeatPayloadFuture.complete(payload);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingHeartbeatTarget();

        final TestingHeartbeatListener<Void, Integer> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<Void, Integer>()
                        .setRetrievePayloadFunction(payloads::get)
                        .createNewTestingHeartbeatListener();

        HeartbeatManager<?, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        ResourceID.generate(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
            heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

            heartbeatManager.requestHeartbeat(someTargetId, null);
            assertThat(someHeartbeatPayloadFuture.get(), is(payloads.get(someTargetId)));

            heartbeatManager.requestHeartbeat(specialTargetId, null);
            assertThat(specialHeartbeatPayloadFuture.get(), is(payloads.get(specialTargetId)));
        } finally {
            heartbeatManager.stop();
        }
    }

    /**
     * Tests that the heartbeat target {@link ResourceID} is properly passed to the {@link
     * HeartbeatListener} by the {@link HeartbeatManagerSenderImpl}.
     */
    @Test
    public void testHeartbeatManagerSenderTargetPayload() throws Exception {
        final long heartbeatTimeout = 100L;
        final long heartbeatPeriod = 2000L;

        final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(1);

        final ResourceID someTargetId = ResourceID.generate();
        final ResourceID specialTargetId = ResourceID.generate();

        final OneShotLatch someTargetReceivedLatch = new OneShotLatch();
        final OneShotLatch specialTargetReceivedLatch = new OneShotLatch();

        final TargetDependentHeartbeatReceiver someHeartbeatTarget =
                new TargetDependentHeartbeatReceiver(someTargetReceivedLatch);
        final TargetDependentHeartbeatReceiver specialHeartbeatTarget =
                new TargetDependentHeartbeatReceiver(specialTargetReceivedLatch);

        final int defaultResponse = 0;
        final int specialResponse = 1;

        HeartbeatManager<?, Integer> heartbeatManager =
                new HeartbeatManagerSenderImpl<>(
                        heartbeatPeriod,
                        heartbeatTimeout,
                        FAILED_RPC_THRESHOLD,
                        ResourceID.generate(),
                        new TargetDependentHeartbeatSender(
                                specialTargetId, specialResponse, defaultResponse),
                        new ScheduledExecutorServiceAdapter(scheduledThreadPoolExecutor),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
            heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

            someTargetReceivedLatch.await(5, TimeUnit.SECONDS);
            specialTargetReceivedLatch.await(5, TimeUnit.SECONDS);

            assertEquals(defaultResponse, someHeartbeatTarget.getLastRequestedHeartbeatPayload());
            assertEquals(
                    specialResponse, specialHeartbeatTarget.getLastRequestedHeartbeatPayload());
        } finally {
            heartbeatManager.stop();
            scheduledThreadPoolExecutor.shutdown();
        }
    }

    @Test
    public void testHeartbeatManagerSenderMarksTargetUnreachableOnRecipientUnreachableException() {
        final long heartbeatPeriod = 20;
        final long heartbeatTimeout = 10000L;
        final ResourceID someTargetId = ResourceID.generate();
        final HeartbeatTarget<Object> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<>()
                        .setRequestHeartbeatFunction(
                                (ignoredA, ignoredB) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "could not receive heartbeat")))
                        .createTestingHeartbeatTarget();
        final CompletableFuture<ResourceID> unreachableTargetFuture = new CompletableFuture<>();
        final HeartbeatListener<Object, Object> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<>()
                        .setNotifyTargetUnreachableConsumer(unreachableTargetFuture::complete)
                        .createNewTestingHeartbeatListener();

        final HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerSenderImpl<>(
                        heartbeatPeriod,
                        heartbeatTimeout,
                        1,
                        ResourceID.generate(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);

            // the target should become unreachable when requesting a heartbeat
            unreachableTargetFuture.join();
        } finally {
            heartbeatManager.stop();
        }
    }

    @Test
    public void testHeartbeatManagerMarksTargetUnreachableOnRecipientUnreachableException() {
        final long heartbeatTimeout = 10000L;
        final ResourceID someTargetId = ResourceID.generate();
        final HeartbeatTarget<Object> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<>()
                        .setReceiveHeartbeatFunction(
                                (ignoredA, ignoredB) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "could not receive heartbeat")))
                        .createTestingHeartbeatTarget();
        final CompletableFuture<ResourceID> unreachableTargetFuture = new CompletableFuture<>();
        final HeartbeatListener<Object, Object> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<>()
                        .setNotifyTargetUnreachableConsumer(unreachableTargetFuture::complete)
                        .createNewTestingHeartbeatListener();

        final int failedRpcRequestsUntilUnreachable = 5;
        final HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        failedRpcRequestsUntilUnreachable,
                        ResourceID.generate(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);

            for (int i = 0; i < failedRpcRequestsUntilUnreachable - 1; i++) {
                heartbeatManager.requestHeartbeat(someTargetId, null);

                assertThat(
                        unreachableTargetFuture,
                        FlinkMatchers.willNotComplete(willNotCompleteWithin));
            }

            heartbeatManager.requestHeartbeat(someTargetId, null);

            // the target should be unreachable now
            unreachableTargetFuture.join();
        } finally {
            heartbeatManager.stop();
        }
    }

    @Test
    public void testHeartbeatManagerIgnoresRecipientUnreachableExceptionIfDisabled()
            throws Exception {
        final long heartbeatTimeout = 10000L;
        final ResourceID someTargetId = ResourceID.generate();
        final HeartbeatTarget<Object> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<>()
                        .setReceiveHeartbeatFunction(
                                (ignoredA, ignoredB) ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "could not receive heartbeat")))
                        .createTestingHeartbeatTarget();
        final CompletableFuture<ResourceID> unreachableTargetFuture = new CompletableFuture<>();
        final HeartbeatListener<Object, Object> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<>()
                        .setNotifyTargetUnreachableConsumer(unreachableTargetFuture::complete)
                        .createNewTestingHeartbeatListener();

        final HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        -1, // disable rpc request checking
                        ResourceID.generate(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);

            for (int i = 0; i < 10; i++) {
                heartbeatManager.requestHeartbeat(someTargetId, null);
            }

            assertThat(
                    unreachableTargetFuture, FlinkMatchers.willNotComplete(willNotCompleteWithin));
        } finally {
            heartbeatManager.stop();
        }
    }

    @Test
    public void testHeartbeatManagerResetsFailedRpcCountOnSuccessfulRpc() throws Exception {
        final long heartbeatTimeout = 10000L;
        final ResourceID someTargetId = ResourceID.generate();

        final RecipientUnreachableException unreachableException =
                new RecipientUnreachableException(
                        "sender", "recipient", "could not receive heartbeat");
        final Queue<CompletableFuture<Void>> heartbeatResponses =
                new ArrayDeque<>(
                        Arrays.asList(
                                FutureUtils.completedExceptionally(unreachableException),
                                FutureUtils.completedExceptionally(unreachableException),
                                CompletableFuture.completedFuture(null),
                                FutureUtils.completedExceptionally(unreachableException)));
        final HeartbeatTarget<Object> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<>()
                        .setReceiveHeartbeatFunction(
                                (ignoredA, ignoredB) -> heartbeatResponses.poll())
                        .createTestingHeartbeatTarget();
        final CompletableFuture<ResourceID> unreachableTargetFuture = new CompletableFuture<>();
        final HeartbeatListener<Object, Object> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<>()
                        .setNotifyTargetUnreachableConsumer(unreachableTargetFuture::complete)
                        .createNewTestingHeartbeatListener();

        final HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        3,
                        ResourceID.generate(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);

            for (int i = 0; i < heartbeatResponses.size(); i++) {
                heartbeatManager.requestHeartbeat(someTargetId, null);
            }

            assertThat(
                    unreachableTargetFuture, FlinkMatchers.willNotComplete(willNotCompleteWithin));
        } finally {
            heartbeatManager.stop();
        }
    }

    /** Test {@link HeartbeatTarget} that exposes the last received payload. */
    private static class TargetDependentHeartbeatReceiver implements HeartbeatTarget<Integer> {

        private volatile int lastReceivedHeartbeatPayload = -1;
        private volatile int lastRequestedHeartbeatPayload = -1;

        private final OneShotLatch latch;

        public TargetDependentHeartbeatReceiver() {
            this(new OneShotLatch());
        }

        public TargetDependentHeartbeatReceiver(OneShotLatch latch) {
            this.latch = latch;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(
                ResourceID heartbeatOrigin, Integer heartbeatPayload) {
            this.lastReceivedHeartbeatPayload = heartbeatPayload;
            latch.trigger();
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID requestOrigin, Integer heartbeatPayload) {
            this.lastRequestedHeartbeatPayload = heartbeatPayload;
            latch.trigger();
            return FutureUtils.completedVoidFuture();
        }

        public int getLastReceivedHeartbeatPayload() {
            return lastReceivedHeartbeatPayload;
        }

        public int getLastRequestedHeartbeatPayload() {
            return lastRequestedHeartbeatPayload;
        }
    }

    /**
     * Test {@link HeartbeatListener} that returns different payloads based on the target {@link
     * ResourceID}.
     */
    private static class TargetDependentHeartbeatSender
            implements HeartbeatListener<Object, Integer> {
        private final ResourceID specialId;
        private final int specialResponse;
        private final int defaultResponse;

        TargetDependentHeartbeatSender(
                ResourceID specialId, int specialResponse, int defaultResponse) {
            this.specialId = specialId;
            this.specialResponse = specialResponse;
            this.defaultResponse = defaultResponse;
        }

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {}

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {}

        @Override
        public void reportPayload(ResourceID resourceID, Object payload) {}

        @Override
        public Integer retrievePayload(ResourceID resourceID) {
            if (resourceID.equals(specialId)) {
                return specialResponse;
            } else {
                return defaultResponse;
            }
        }
    }
}
