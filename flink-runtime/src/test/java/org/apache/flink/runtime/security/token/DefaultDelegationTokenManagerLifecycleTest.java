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

package org.apache.flink.runtime.security.token;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the two lifetimes of the {@link DefaultDelegationTokenManager}: obtaining the tokens
 * lasts as long as the process, a listener which distributes them lasts as long as a leader
 * session.
 */
class DefaultDelegationTokenManagerLifecycleTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** The default obtain time ratio of 0.75 applied to the 60 seconds a token lives. */
    private static final long OBTAIN_DELAY_MILLIS = 45_000L;

    private final MockTokenService tokenService =
            new MockTokenService(
                    Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC),
                    Duration.ofSeconds(60));
    private final DelegationTokenManagerTestHarness harness =
            new DelegationTokenManagerTestHarness(tokenService);
    private final DefaultDelegationTokenManager manager = harness.manager();
    private final MockDelegationTokenProvider provider = harness.provider();
    private final MockDelegationTokenReceiver receiver = harness.receiver();

    // ------------------------------------------------------------------------------------------
    // Obtaining the tokens for the process
    // ------------------------------------------------------------------------------------------

    @Test
    void startObtainsTokensForTheReceiversAndSchedulesTheNextObtainWithoutAnyListener()
            throws Exception {
        manager.start();

        assertThat(provider.obtainCalls()).isEqualTo(1);
        assertThat(receiver.receivedGenerations()).containsExactly(1);
        assertThat(harness.pendingObtainDelaysMillis()).containsExactly(OBTAIN_DELAY_MILLIS);
    }

    @Test
    void scheduledObtainsKeepUpdatingTheReceiversWithoutAnyListener() throws Exception {
        manager.start();

        harness.obtainIsDue();
        harness.obtainIsDue();

        assertThat(provider.obtainCalls()).isEqualTo(3);
        assertThat(receiver.receivedGenerations()).containsExactly(1, 2, 3);
        assertThat(harness.pendingObtainDelaysMillis()).containsExactly(OBTAIN_DELAY_MILLIS);
    }

    @Test
    void startWithoutRequiredTokensObtainsNothingAndSchedulesNothing() throws Exception {
        provider.setTokensRequired(false);

        manager.start();
        final RecordingListener listener = new RecordingListener();
        manager.attachListener(listener);

        assertThat(provider.obtainCalls()).isZero();
        assertThat(receiver.receivedGenerations()).isEmpty();
        assertThat(listener.receivedGenerations()).isEmpty();
        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // The listener of a leader session
    // ------------------------------------------------------------------------------------------

    @Test
    void attachHandsOverTheLatestTokensWithoutContactingTheTokenService() throws Exception {
        manager.start();
        harness.obtainIsDue();
        final RecordingListener listener = new RecordingListener();

        manager.attachListener(listener);

        assertThat(listener.receivedGenerations()).containsExactly(2);
        assertThat(provider.obtainCalls()).isEqualTo(2);
    }

    @Test
    void attachDoesNotHandOverTokensWhichExpired() throws Exception {
        final AdjustableClock clock = new AdjustableClock(1_000_000L);
        final DelegationTokenManagerTestHarness expiring =
                new DelegationTokenManagerTestHarness(
                        new MockTokenService(clock, Duration.ofSeconds(60)));
        expiring.manager().start();
        // The token service stays unreachable until the tokens which the process holds expired.
        expiring.provider().failWith(new IllegalStateException("token service unreachable"));
        clock.advance(Duration.ofSeconds(61));
        expiring.obtainIsDue();
        final RecordingListener listener = new RecordingListener();

        expiring.manager().attachListener(listener);

        assertThat(listener.receivedGenerations()).isEmpty();

        expiring.provider().failWith(null);
        expiring.obtainIsDue();

        assertThat(listener.receivedGenerations()).containsExactly(2);
    }

    @Test
    void obtainedTokensAreDeliveredToTheAttachedListenerInOrder() throws Exception {
        manager.start();
        final RecordingListener listener = new RecordingListener();
        manager.attachListener(listener);

        harness.obtainIsDue();
        harness.obtainIsDue();

        assertThat(listener.receivedGenerations()).containsExactly(1, 2, 3);
    }

    @Test
    void detachEndsTheDeliveryButTheObtainingContinues() throws Exception {
        manager.start();
        final RecordingListener listener = new RecordingListener();
        manager.attachListener(listener);

        manager.detachListener(listener);
        harness.obtainIsDue();

        assertThat(listener.receivedGenerations()).containsExactly(1);
        assertThat(receiver.receivedGenerations()).containsExactly(1, 2);
        assertThat(provider.obtainCalls()).isEqualTo(2);
        assertThat(harness.pendingObtainDelaysMillis()).containsExactly(OBTAIN_DELAY_MILLIS);
    }

    @Test
    void nextLeaderSessionGetsTheTokensObtainedWhileNobodyWasAttached() throws Exception {
        manager.start();
        final RecordingListener firstSession = new RecordingListener();
        manager.attachListener(firstSession);
        harness.obtainIsDue();
        manager.detachListener(firstSession);
        harness.obtainIsDue();

        final RecordingListener secondSession = new RecordingListener();
        manager.attachListener(secondSession);

        assertThat(firstSession.receivedGenerations()).containsExactly(1, 2);
        assertThat(secondSession.receivedGenerations()).containsExactly(3);
        assertThat(provider.obtainCalls()).isEqualTo(3);
    }

    @Test
    void detachOfAnEarlierListenerKeepsTheCurrentOne() throws Exception {
        manager.start();
        final RecordingListener earlierSession = new RecordingListener();
        final RecordingListener currentSession = new RecordingListener();
        manager.attachListener(earlierSession);
        manager.attachListener(currentSession);

        manager.detachListener(earlierSession);
        harness.obtainIsDue();

        assertThat(currentSession.receivedGenerations()).containsExactly(1, 2);
        assertThat(earlierSession.receivedGenerations()).containsExactly(1);
    }

    @Test
    void obtainDuringTheReplayOfAnAttachIsDeliveredAfterTheReplay() throws Exception {
        manager.start();
        final CountDownLatch replayInListener = new CountDownLatch(1);
        final CountDownLatch replayReleased = new CountDownLatch(1);
        final RecordingListener listener =
                new RecordingListener(
                        generation -> {
                            if (generation == 1) {
                                replayInListener.countDown();
                                replayReleased.await();
                            }
                        });
        final ExecutorService attacher = Executors.newSingleThreadExecutor();
        final ExecutorService obtainer = Executors.newSingleThreadExecutor();
        try {
            final Future<?> attach =
                    attacher.submit(
                            () -> {
                                manager.attachListener(listener);
                                return null;
                            });
            assertThat(replayInListener.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("the replay reached the listener")
                    .isTrue();

            final Thread obtainerThread = obtainer.submit(Thread::currentThread).get();
            final Future<?> obtain = obtainer.submit(harness::obtainIsDue);
            // The obtain must queue up behind the replay. A manager which lets it pass delivers
            // the new tokens first, which is what the assertion below detects.
            waitUtil(
                    () -> obtain.isDone() || obtainerThread.getState() == Thread.State.BLOCKED,
                    TIMEOUT,
                    "The obtain neither completed nor waits for the replay");
            replayReleased.countDown();
            attach.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            obtain.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(listener.receivedGenerations()).containsExactly(1, 2);
        } finally {
            replayReleased.countDown();
            attacher.shutdownNow();
            obtainer.shutdownNow();
        }
    }

    @Test
    void detachWaitsForTheDeliveryInProgressAndNothingIsDeliveredAfterwards() throws Exception {
        manager.start();
        final AtomicBoolean holdNextCallback = new AtomicBoolean();
        final CountDownLatch obtainInListener = new CountDownLatch(1);
        final CountDownLatch obtainReleased = new CountDownLatch(1);
        final RecordingListener listener =
                new RecordingListener(
                        generation -> {
                            if (holdNextCallback.compareAndSet(true, false)) {
                                obtainInListener.countDown();
                                obtainReleased.await();
                            }
                        });
        manager.attachListener(listener);
        // Only the callback of the obtain is held, a callback on the test thread would hang it.
        holdNextCallback.set(true);
        final ExecutorService obtainer = Executors.newSingleThreadExecutor();
        final ExecutorService detacher = Executors.newSingleThreadExecutor();
        try {
            final Future<?> obtain = obtainer.submit(harness::obtainIsDue);
            assertThat(obtainInListener.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("the obtain reached the listener")
                    .isTrue();

            final Thread detacherThread = detacher.submit(Thread::currentThread).get();
            final Future<?> detach = detacher.submit(() -> manager.detachListener(listener));
            // The detach must wait for the callback in progress. A manager which lets it pass
            // returns here while the callback is still held.
            waitUtil(
                    () -> detach.isDone() || detacherThread.getState() == Thread.State.BLOCKED,
                    TIMEOUT,
                    "The detach neither returned nor waits for the callback in progress");
            assertThat(detach).as("detach returned during a callback").isNotDone();

            obtainReleased.countDown();
            obtain.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            detach.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(listener.receivedGenerations()).containsExactly(1, 2);

            harness.obtainIsDue();
            assertThat(listener.receivedGenerations()).containsExactly(1, 2);
        } finally {
            obtainReleased.countDown();
            obtainer.shutdownNow();
            detacher.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Stop
    // ------------------------------------------------------------------------------------------

    @Test
    void stopCancelsTheScheduledObtainAndReleasesTheListener() throws Exception {
        manager.start();
        final RecordingListener listener = new RecordingListener();
        manager.attachListener(listener);

        manager.stop();
        harness.obtainIsDue();

        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
        assertThat(provider.obtainCalls()).isEqualTo(1);
        assertThat(listener.receivedGenerations()).containsExactly(1);
        assertThatThrownBy(() -> manager.attachListener(new RecordingListener()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be running to attach a listener")
                .hasMessageContaining("STOPPED");
    }

    @Test
    void stopPreventsAnObtainWhichTheTimerAlreadyHandedToTheIoExecutor() throws Exception {
        manager.start();
        harness.timerHandsTheObtainToTheIoExecutor();

        manager.stop();
        harness.ioExecutorRunsItsTasks();

        assertThat(provider.obtainCalls()).isEqualTo(1);
        assertThat(receiver.receivedGenerations()).containsExactly(1);
        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
    }

    @Test
    void stopDuringAnObtainNeitherReschedulesNorDeliversItsTokens() throws Exception {
        manager.start();
        final RecordingListener listener = new RecordingListener();
        manager.attachListener(listener);
        provider.duringObtain(manager::stop);

        harness.obtainIsDue();

        assertThat(provider.obtainCalls()).isEqualTo(2);
        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
        assertThat(listener.receivedGenerations()).containsExactly(1);
        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("can only be started once")
                .hasMessageContaining("STOPPED");
    }

    // ------------------------------------------------------------------------------------------
    // Failures
    // ------------------------------------------------------------------------------------------

    @Test
    void failureOfTheFirstObtainFailsTheStartAndStopsTheManager() {
        provider.failWith(new IllegalStateException("token service unreachable"));

        assertThatThrownBy(manager::start)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("token service unreachable");

        assertThat(provider.obtainCalls()).isEqualTo(1);
        assertThat(receiver.receivedGenerations()).isEmpty();
        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
        assertThatThrownBy(() -> manager.attachListener(new RecordingListener()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("STOPPED");
    }

    @Test
    void errorOfTheFirstObtainIsNotWrappedAndStopsTheManager() {
        provider.duringObtain(
                () -> {
                    throw new NoClassDefFoundError("provider library is missing");
                });

        assertThatThrownBy(manager::start)
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("provider library is missing");

        assertThat(harness.pendingObtainDelaysMillis()).isEmpty();
        assertThatThrownBy(() -> manager.attachListener(new RecordingListener()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("STOPPED");
    }

    @Test
    void failedObtainIsRetriedBeforeTheTokensExpire() throws Exception {
        // The first retry without the cap is 5 to 15 seconds (10 seconds initial backoff, jitter of
        // 50%). The token lives 3 seconds, so only the cap of a third of the remaining validity
        // keeps the retry below the 1 second asserted here.
        final Duration tokenLifetime = Duration.ofSeconds(3);
        final DelegationTokenManagerTestHarness shortLived =
                new DelegationTokenManagerTestHarness(
                        new MockTokenService(
                                Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC),
                                tokenLifetime));
        shortLived.manager().start();
        final RecordingListener listener = new RecordingListener();
        shortLived.manager().attachListener(listener);

        shortLived.provider().failWith(new IllegalStateException("token service unreachable"));
        shortLived.obtainIsDue();

        assertThat(shortLived.receiver().receivedGenerations()).containsExactly(1);
        assertThat(shortLived.pendingObtainDelaysMillis())
                .hasSize(1)
                .allSatisfy(retryDelay -> assertThat(retryDelay).isBetween(0L, 1_000L));

        shortLived.provider().failWith(null);
        shortLived.obtainIsDue();

        assertThat(shortLived.receiver().receivedGenerations()).containsExactly(1, 2);
        assertThat(listener.receivedGenerations()).containsExactly(1, 2);
        assertThat(shortLived.pendingObtainDelaysMillis())
                .containsExactly(Math.round(0.75 * tokenLifetime.toMillis()));
    }

    @Test
    void failingListenerKeepsTheReceiversUpdatedAndTheObtainIsRetried() throws Exception {
        manager.start();
        final RecordingListener listener =
                new RecordingListener(
                        generation -> {
                            if (generation == 2) {
                                throw new IllegalStateException("task manager unreachable");
                            }
                        });
        manager.attachListener(listener);

        harness.obtainIsDue();

        assertThat(receiver.receivedGenerations()).containsExactly(1, 2);
        assertThat(listener.receivedGenerations()).containsExactly(1);
        assertThat(harness.pendingObtainDelaysMillis()).hasSize(1);

        harness.obtainIsDue();

        assertThat(receiver.receivedGenerations()).containsExactly(1, 2, 3);
        assertThat(listener.receivedGenerations()).containsExactly(1, 3);
    }

    // ------------------------------------------------------------------------------------------
    // Misuse
    // ------------------------------------------------------------------------------------------

    @Test
    void attachBeforeStartAndSecondStartAreRejected() throws Exception {
        assertThatThrownBy(() -> manager.attachListener(new RecordingListener()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be running to attach a listener")
                .hasMessageContaining("CREATED");

        manager.start();

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("can only be started once")
                .hasMessageContaining("RUNNING");
        assertThat(provider.obtainCalls()).isEqualTo(1);
        assertThat(harness.pendingObtainDelaysMillis()).containsExactly(OBTAIN_DELAY_MILLIS);
    }

    /** A clock which only moves when the test says so. */
    private static final class AdjustableClock extends Clock {

        private volatile long millis;

        AdjustableClock(long millis) {
            this.millis = millis;
        }

        void advance(Duration duration) {
            millis += duration.toMillis();
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }
    }

    /** Stands for the ResourceManager of a leader session and remembers what it was handed. */
    private static final class RecordingListener implements DelegationTokenManager.Listener {

        private final List<Integer> receivedGenerations = new CopyOnWriteArrayList<>();
        private final ThrowingConsumer<Integer, Exception> beforeRecording;

        RecordingListener() {
            this(generation -> {});
        }

        /** The action runs before the tokens are recorded, it can hold the callback or fail it. */
        RecordingListener(ThrowingConsumer<Integer, Exception> beforeRecording) {
            this.beforeRecording = beforeRecording;
        }

        List<Integer> receivedGenerations() {
            return receivedGenerations;
        }

        @Override
        public void onNewTokensObtained(byte[] tokens) throws Exception {
            final int generation = MockTokenService.generationIn(tokens);
            beforeRecording.accept(generation);
            receivedGenerations.add(generation);
        }
    }
}
