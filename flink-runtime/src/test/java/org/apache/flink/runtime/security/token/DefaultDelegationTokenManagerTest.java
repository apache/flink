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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_REOBTAIN_COOLDOWN;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class DefaultDelegationTokenManagerTest {

    @BeforeEach
    public void beforeEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @AfterEach
    public void afterEach() {
        ExceptionThrowingDelegationTokenProvider.reset();
        ExceptionThrowingDelegationTokenReceiver.reset();
    }

    @Test
    public void isProviderEnabledMustGiveBackTrueByDefault() {
        Configuration configuration = new Configuration();

        assertTrue(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test"));
    }

    @Test
    public void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".test.enabled"), false);

        assertFalse(DefaultDelegationTokenManager.isProviderEnabled(configuration, "test"));
    }

    @Test
    public void configurationIsNullMustFailFast() {
        assertThrows(
                Exception.class, () -> new DefaultDelegationTokenManager(null, null, null, null));
    }

    @Test
    public void oneProviderThrowsExceptionMustFailFast() {
        assertThrows(
                Exception.class,
                () -> {
                    ExceptionThrowingDelegationTokenProvider.throwInInit.set(true);
                    new DefaultDelegationTokenManager(new Configuration(), null, null, null);
                });
    }

    @Test
    public void testAllProvidersLoaded() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        assertEquals(3, delegationTokenManager.delegationTokenProviders.size());

        assertTrue(delegationTokenManager.isProviderLoaded("hadoopfs"));
        assertTrue(delegationTokenManager.isReceiverLoaded("hadoopfs"));

        assertTrue(delegationTokenManager.isProviderLoaded("hbase"));
        assertTrue(delegationTokenManager.isReceiverLoaded("hbase"));

        assertTrue(delegationTokenManager.isProviderLoaded("test"));
        assertTrue(delegationTokenManager.isReceiverLoaded("test"));

        assertTrue(ExceptionThrowingDelegationTokenProvider.constructed.get());
        assertTrue(ExceptionThrowingDelegationTokenReceiver.constructed.get());
        assertFalse(delegationTokenManager.isProviderLoaded("throw"));
        assertFalse(delegationTokenManager.isReceiverLoaded("throw"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldNotThrowWhenNothingLoaded() {
        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldThrowWhenMissingReceiver() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());

        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        providers, Collections.emptyMap()));
        assertTrue(e.getMessage().contains("Missing receivers: test"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldThrowWhenMissingProvider() {
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(
                                        Collections.emptyMap(), receivers));
        assertTrue(e.getMessage().contains("Missing providers: test"));
    }

    @Test
    public void checkProviderAndReceiverConsistencyShouldNotThrowWhenBothLoaded() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("test", new TestDelegationTokenProvider());
        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        receivers.put("test", new TestDelegationTokenReceiver());

        DefaultDelegationTokenManager.checkProviderAndReceiverConsistency(providers, receivers);

        assertEquals(1, providers.size());
        assertTrue(providers.containsKey("test"));
        assertEquals(1, receivers.size());
        assertTrue(receivers.containsKey("test"));
    }

    @Test
    public void checkSamePrefixedProvidersShouldNotGiveErrorsWhenNoSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertTrue(warnings.isEmpty());
    }

    @Test
    public void checkSamePrefixedProvidersShouldGiveErrorsWhenSamePrefix() {
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        providers.put("s3-hadoop", new TestDelegationTokenProvider());
        providers.put("s3-presto", new TestDelegationTokenProvider());
        Set<String> warnings = new HashSet<>();
        DefaultDelegationTokenManager.checkSamePrefixedProviders(providers, warnings);
        assertEquals(1, warnings.size());
        assertEquals(
                "Multiple providers loaded with the same prefix: s3. This might lead to unintended consequences, please consider using only one of them.",
                warnings.iterator().next());
    }

    @Test
    public void startTokensUpdateShouldScheduleRenewal() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        ExceptionThrowingDelegationTokenProvider.addToken.set(true);
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), true);
        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };

        delegationTokenManager.startTokensUpdate();
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(true);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(false);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        delegationTokenManager.stopTokensUpdate();

        assertEquals(3, startTokensUpdateCallCount.get());
    }

    @Test
    public void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock constantClock = Clock.fixed(ofEpochMilli(100), ZoneId.systemDefault());
        assertEquals(50, delegationTokenManager.calculateRenewalDelay(constantClock, 200));
    }

    @Test
    public void calculateRetryDelayShouldDoubleOnConsecutiveFailures() {
        long initialMs = Duration.ofSeconds(10).toMillis();
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        long delay1 = manager.calculateRetryDelay(clock);
        long delay2 = manager.calculateRetryDelay(clock);
        long delay3 = manager.calculateRetryDelay(clock);

        // Each delay should be within [0, 2 * initial * 2^(n-1)] accounting for ±50% jitter.
        assertTrue(delay1 >= 0 && delay1 <= initialMs * 2);
        assertTrue(delay2 >= 0 && delay2 <= initialMs * 4);
        assertTrue(delay3 >= 0 && delay3 <= initialMs * 8);
        // The base must have doubled: currentRetryBackoff after 3 calls is min(80s, 5min) = 80s.
        assertEquals(Duration.ofSeconds(80).toMillis(), manager.currentRetryBackoff);
    }

    @Test
    public void calculateRetryDelayShouldResetAfterSuccess() {
        long initialMs = Duration.ofSeconds(10).toMillis();
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        // Ramp up the backoff via two failures.
        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        manager.calculateRetryDelay(clock);
        manager.calculateRetryDelay(clock);
        // Simulate success: reset currentRetryBackoff (as startTokensUpdate() would).
        manager.currentRetryBackoff = initialMs;

        long delayAfterReset = manager.calculateRetryDelay(clock);
        assertTrue(delayAfterReset >= 0 && delayAfterReset <= initialMs * 2);
        assertEquals(initialMs * 2, manager.currentRetryBackoff);
    }

    @Test
    public void calculateRetryDelayShouldCapToTtlBound() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(10));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofMinutes(5));
        DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        // Simulate a failure close to token expiry (30 s remaining). The delay must be capped
        // so that the retry happens while the token is still valid (at most 30 s / 3 = 10 s).
        Clock clock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        manager.lastKnownNextRenewal = Duration.ofSeconds(30).toMillis();

        long delay = manager.calculateRetryDelay(clock);

        // Delay must not exceed the TTL cap (30 s / 3 = 10 s), with jitter the max is 10 s.
        assertTrue(delay <= Duration.ofSeconds(10).toMillis());
        assertTrue(delay >= 0);
    }

    @Test
    public void registerJobShouldTriggerImmediateRenewalAndTrackJob() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), true);
        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };
        // Ask the provider to request an immediate refresh when the job is registered.
        ExceptionThrowingDelegationTokenProvider.shouldReobtainOnRegister.set(true);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        assertEquals(1, startTokensUpdateCallCount.get());
        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());

        delegationTokenManager.unregisterJob(jobId);
        assertEquals(0, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());
    }

    @Test
    public void stopShouldStopProviders() {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        delegationTokenManager.stop();

        assertTrue(ExceptionThrowingDelegationTokenProvider.stopped.get());
    }

    @Test
    public void registerJobShouldRollBackAndRethrowWhenProviderThrows() throws Exception {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());

        // A provider that throws during registration must cause the job to be unregistered from
        // all providers and the exception to be rethrown.
        ExceptionThrowingDelegationTokenProvider.throwInRegister.set(true);
        assertThrows(
                IllegalArgumentException.class,
                () -> delegationTokenManager.registerJob(jobId, new Configuration()));
        assertEquals(0, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());
    }

    @Test
    public void unregisterJobShouldSwallowProviderFailure() throws Exception {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());

        // A provider that throws during unregistration must not prevent cleanup from completing.
        ExceptionThrowingDelegationTokenProvider.throwInUnregister.set(true);
        assertDoesNotThrow(() -> delegationTokenManager.unregisterJob(jobId));
    }

    @Test
    public void reobtainShouldCoalesceConcurrentRequests() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        new Configuration(), null, scheduledExecutor, scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };

        // Two requests before the cycle runs must be coalesced into a single scheduled obtain.
        delegationTokenManager.reobtainDelegationTokens();
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(1, scheduledExecutor.getActiveScheduledTasks().size());
        // The second request must be a true no-op: it must not cancel and reschedule a new future
        // (which would also leave a single *active* task). getAllScheduledTasks() includes
        // cancelled futures, so it stays 1 only if the second request was genuinely coalesced.
        assertEquals(1, scheduledExecutor.getAllScheduledTasks().size());

        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        assertEquals(1, startTokensUpdateCallCount.get());
    }

    @Test
    public void periodicRenewalMustNotCancelPendingOnDemandReobtain() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        new Configuration(), null, scheduledExecutor, scheduler);

        // An on-demand re-obtain is scheduled (e.g. a freshly registered job).
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(1, scheduledExecutor.getAllScheduledTasks().size());
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));

        // A periodic obtain cycle that was already running completes and tries to install its own
        // renewal. It must NOT cancel the pending on-demand re-obtain (regression test for the
        // lost-reobtain race that also latched the dedupe flag).
        delegationTokenManager.maybeScheduleRenewal(999_999L);

        // No cancel+reschedule happened (still a single schedule call) and the pending future is
        // still the immediate on-demand one, not the 999_999ms periodic renewal.
        assertEquals(1, scheduledExecutor.getAllScheduledTasks().size());
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));

        // Once the on-demand cycle has run and cleared the dedupe flag, a periodic renewal can be
        // scheduled normally again.
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        delegationTokenManager.maybeScheduleRenewal(123L);
        assertEquals(123L, onlyScheduledDelayMillis(scheduledExecutor));
    }

    @Test
    public void reobtainShouldRunImmediatelyAfterCooldownWindowElapses() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler);

        long t0 = 1_000_000L;
        delegationTokenManager.setClock(Clock.fixed(ofEpochMilli(t0), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // A request arriving after the full cooldown window has elapsed runs immediately again.
        delegationTokenManager.setClock(
                Clock.fixed(ofEpochMilli(t0 + 70_000L), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
    }

    @Test
    public void stopShouldResetCooldownForSubsequentStart() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler);

        long t0 = 1_000_000L;
        delegationTokenManager.setClock(Clock.fixed(ofEpochMilli(t0), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s later a re-obtain is deferred by the cooldown.
        delegationTokenManager.setClock(
                Clock.fixed(ofEpochMilli(t0 + 10_000L), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(50_000L, onlyScheduledDelayMillis(scheduledExecutor));

        // stop() clears the cooldown anchor (and the dedupe/stopped state). After a restart, the
        // next re-obtain runs immediately instead of inheriting the stale cooldown.
        delegationTokenManager.stop();
        delegationTokenManager.start(tokens -> {});
        delegationTokenManager.setClock(
                Clock.fixed(ofEpochMilli(t0 + 15_000L), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
    }

    @Test
    public void reobtainShouldRespectCooldown() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler);

        long t0 = 1_000_000L;
        delegationTokenManager.setClock(Clock.fixed(ofEpochMilli(t0), ZoneId.systemDefault()));

        // First re-obtain after a quiet period runs immediately (no cooldown applies).
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // A second re-obtain 10s later must be deferred until the 60s cooldown elapses.
        delegationTokenManager.setClock(
                Clock.fixed(ofEpochMilli(t0 + 10_000L), ZoneId.systemDefault()));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(50_000L, onlyScheduledDelayMillis(scheduledExecutor));
    }

    @Test
    public void reobtainShouldBeIgnoredWhenNotStarted() {
        // Constructed with null executors, so never started. A re-obtain request must be a safe
        // no-op.
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        assertDoesNotThrow(delegationTokenManager::reobtainDelegationTokens);
    }

    @Test
    public void registerJobShouldBeIdempotent() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        // Re-registering the same job (e.g. on JobManager/ResourceManager failover) must not
        // accumulate duplicate per-job state in the providers.
        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        delegationTokenManager.registerJob(jobId, new Configuration());

        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());
    }

    @Test
    public void obtainLockSerializesConcurrentObtainCycles() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ExecutorService ioExecutor = Executors.newFixedThreadPool(2);
        try {
            // The barrier trips only if two obtain cycles are inside the obtain/broadcast
            // section at the same time. obtainLock must serialize them, so each should time out.
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean concurrentObtainDetected = new AtomicBoolean(false);
            final CountDownLatch done = new CountDownLatch(2);

            DefaultDelegationTokenManager delegationTokenManager =
                    new DefaultDelegationTokenManager(
                            new Configuration(), null, scheduledExecutor, ioExecutor) {
                        @Override
                        protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                                DelegationTokenContainer container) {
                            try {
                                barrier.await(200, TimeUnit.MILLISECONDS);
                                // Reached only if both cycles met here concurrently.
                                concurrentObtainDetected.set(true);
                            } catch (TimeoutException | BrokenBarrierException serialized) {
                                // Expected: the other cycle never entered within the window.
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return Optional.empty();
                        }
                    };

            ioExecutor.execute(
                    () -> {
                        delegationTokenManager.startTokensUpdate();
                        done.countDown();
                    });
            ioExecutor.execute(
                    () -> {
                        delegationTokenManager.startTokensUpdate();
                        done.countDown();
                    });

            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertFalse(
                    concurrentObtainDetected.get(),
                    "obtainLock must prevent two obtain cycles from running concurrently");
        } finally {
            ioExecutor.shutdownNow();
        }
    }

    /**
     * Configuration for cooldown-scheduling tests: sets the cooldown and disables all providers
     * that could fail the obtain cycle (hadoopfs/hbase need a real Hadoop setup, and the throw
     * provider fails on demand). A failed cycle schedules a jittered retry, and the bring-forward
     * clamp would coalesce the on-demand request into that retry instead of deferring by the
     * cooldown, making delay assertions nondeterministic.
     */
    private static Configuration hermeticCooldownConfig(Duration cooldown) {
        Configuration configuration = new Configuration();
        configuration.set(DELEGATION_TOKENS_REOBTAIN_COOLDOWN, cooldown);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);
        return configuration;
    }

    private static long onlyScheduledDelayMillis(
            ManuallyTriggeredScheduledExecutor scheduledExecutor) {
        Collection<ScheduledFuture<?>> tasks = scheduledExecutor.getActiveScheduledTasks();
        assertEquals(1, tasks.size());
        return tasks.iterator().next().getDelay(TimeUnit.MILLISECONDS);
    }
}
