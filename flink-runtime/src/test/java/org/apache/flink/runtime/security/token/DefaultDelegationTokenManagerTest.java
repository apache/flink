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
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_REOBTAIN_COOLDOWN;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class DefaultDelegationTokenManagerTest {

    @RegisterExtension
    private final LoggerAuditingExtension loggerAuditingExtension =
            new LoggerAuditingExtension(
                    DefaultDelegationTokenManager.class, org.slf4j.event.Level.DEBUG);

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
    public void startTokensUpdateShouldScheduleRenewal() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        ExceptionThrowingDelegationTokenProvider.addToken.set(true);
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), true);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);
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

        // The first two cycles fail and schedule a retry each. The third succeeds.
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(true);
        delegationTokenManager.start(tokens -> {});
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        ExceptionThrowingDelegationTokenProvider.throwInUsage.set(false);
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        delegationTokenManager.stop();

        assertEquals(3, startTokensUpdateCallCount.get());
    }

    @Test
    public void calculateRenewalDelayShouldConsiderRenewalRatio() {
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 0.5);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        ManualClock constantClock = new ManualClock(100 * 1_000_000L);
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

        ManualClock clock = new ManualClock(0);
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
        ManualClock clock = new ManualClock(0);
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
        ManualClock clock = new ManualClock(0);
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
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);
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

        delegationTokenManager.start(tokens -> {});
        // Only count the cycle triggered by the registration below, not start()'s inline cycle.
        startTokensUpdateCallCount.set(0);

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
    public void closeShouldCloseProvidersExactlyOnce() {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        // close() is the terminal teardown: it closes the providers, and a repeated close() must
        // not close them again (the SPI promises close() is called at most once).
        delegationTokenManager.close();
        delegationTokenManager.close();

        assertTrue(ExceptionThrowingDelegationTokenProvider.closed.get());
        assertEquals(1, (int) ExceptionThrowingDelegationTokenProvider.closeCallCount.get());
    }

    @Test
    public void closeShouldEndSessionAndUnregisterJobs() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());

        // close() ends the session first, so the jobs are unregistered while the providers are
        // still usable, and only then are the providers closed.
        delegationTokenManager.close();

        assertEquals(0, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());
        assertTrue(ExceptionThrowingDelegationTokenProvider.closed.get());
    }

    @Test
    public void startAfterCloseMustFail() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler);
        delegationTokenManager.close();

        // A closed manager's providers are closed for good: starting it would run obtain
        // cycles against dead providers, so it must fail fast.
        assertThrows(IllegalStateException.class, () -> delegationTokenManager.start(tokens -> {}));
    }

    @Test
    public void failedFirstRegistrationMustRethrowWithoutTouchingOtherJobs() throws Exception {
        Configuration configuration = new Configuration();
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(configuration, null, null, null);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());

        // A provider that throws during the FIRST registration of a job must cause that job to
        // be unregistered from all providers and the exception to be rethrown, without touching
        // other jobs' state. (A failed RE-registration keeps the previous registration instead,
        // see failedReregistrationMustNotWipePreviousRegistration.)
        ExceptionThrowingDelegationTokenProvider.throwInRegister.set(true);
        JobID otherJobId = JobID.generate();
        assertThrows(
                IllegalArgumentException.class,
                () -> delegationTokenManager.registerJob(otherJobId, new Configuration()));
        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());
        assertTrue(
                ExceptionThrowingDelegationTokenProvider.registeredJobs.get().contains(jobId),
                "A failed registration of another job must not affect this job's state");
    }

    @Test
    public void registerJobFailureWithLinkageErrorMustRollBackProviders() {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        // A LinkageError from provider plugin code must get the same treatment as an exception:
        // roll back on all providers and rethrow.
        ExceptionThrowingDelegationTokenProvider.throwErrorInRegister.set(true);
        JobID jobId = JobID.generate();

        assertThrows(
                NoClassDefFoundError.class,
                () -> delegationTokenManager.registerJob(jobId, new Configuration()));
        assertTrue(
                ExceptionThrowingDelegationTokenProvider.registeredJobs.get().isEmpty(),
                "A registration that failed with a LinkageError must be rolled back on all"
                        + " providers");
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
    public void unregisterJobShouldSwallowProviderLinkageError() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());

        // A LinkageError during unregistration must be swallowed like an exception, so it does
        // not abort the cleanup of the remaining providers.
        ExceptionThrowingDelegationTokenProvider.throwErrorInUnregister.set(true);
        assertDoesNotThrow(() -> delegationTokenManager.unregisterJob(jobId));
    }

    @Test
    public void reobtainShouldCoalesceConcurrentRequests() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        AtomicInteger startTokensUpdateCallCount = new AtomicInteger(0);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler) {
                    @Override
                    void startTokensUpdate() {
                        startTokensUpdateCallCount.incrementAndGet();
                        super.startTokensUpdate();
                    }
                };
        delegationTokenManager.start(tokens -> {});
        // Only count the cycle serving the coalesced requests, not start()'s inline cycle.
        startTokensUpdateCallCount.set(0);

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
    public void periodicRenewalMustNotCancelPendingOnDemandReobtain() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler);
        delegationTokenManager.start(tokens -> {});

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
    public void reobtainShouldRunImmediatelyAfterCooldownWindowElapses() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler, clock);

        delegationTokenManager.start(tokens -> {});
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // A request arriving after the full cooldown window has elapsed runs immediately again.
        clock.advanceTime(Duration.ofMillis(70_000L));
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
        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler, clock);

        delegationTokenManager.start(tokens -> {});
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s later a re-obtain is deferred by the cooldown.
        clock.advanceTime(Duration.ofMillis(10_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(50_000L, onlyScheduledDelayMillis(scheduledExecutor));

        // stop() clears the cooldown anchor (and the dedupe/stopped state). After a restart, the
        // next re-obtain runs immediately instead of inheriting the stale cooldown.
        delegationTokenManager.stop();
        delegationTokenManager.start(tokens -> {});
        clock.advanceTime(Duration.ofMillis(5_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
    }

    @Test
    public void reobtainShouldRespectCooldown() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler, clock);

        delegationTokenManager.start(tokens -> {});

        // First re-obtain after a quiet period runs immediately (no cooldown applies).
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // A second re-obtain 10s later must be deferred until the 60s cooldown elapses.
        clock.advanceTime(Duration.ofMillis(10_000L));
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
    public void reobtainBeforeStartMustNotScheduleObtainCycle() {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler);

        // Providers receive the re-obtain callback already in the constructor (init), so a
        // provider can invoke it before start(). The manager has no listener yet, so the
        // request must be rejected instead of dispatching an obtain cycle that can only fail
        // on the null listener and keep rescheduling itself through the retry path.
        delegationTokenManager.reobtainDelegationTokens();

        assertEquals(
                0,
                scheduledExecutor.getActiveScheduledTasks().size(),
                "A re-obtain before start() must not schedule an obtain cycle");
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 60_000})
    public void schedulerFailureMustNotWedgeSubsequentReobtains(long cooldownMillis)
            throws Exception {
        final ManuallyTriggeredScheduledExecutor delegate =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        // Throws a plain RuntimeException (not a RejectedExecutionException) on the next
        // schedule() call when the flag is set, then behaves normally again.
        final AtomicBoolean throwNext = new AtomicBoolean(false);
        ScheduledExecutor throwOnce =
                new ScheduledExecutor() {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        if (throwNext.compareAndSet(true, false)) {
                            throw new RuntimeException("simulated scheduler failure");
                        }
                        return delegate.schedule(command, delay, unit);
                    }

                    @Override
                    public <V> ScheduledFuture<V> schedule(
                            Callable<V> callable, long delay, TimeUnit unit) {
                        return delegate.schedule(callable, delay, unit);
                    }

                    @Override
                    public ScheduledFuture<?> scheduleAtFixedRate(
                            Runnable command, long initialDelay, long period, TimeUnit unit) {
                        return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
                    }

                    @Override
                    public ScheduledFuture<?> scheduleWithFixedDelay(
                            Runnable command, long initialDelay, long delay, TimeUnit unit) {
                        return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
                    }

                    @Override
                    public void execute(Runnable command) {
                        delegate.execute(command);
                    }
                };

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(cooldownMillis)),
                        null,
                        throwOnce,
                        scheduler,
                        new ManualClock());
        delegationTokenManager.start(tokens -> {});

        // The first re-obtain hits a scheduler that blows up with something other than the
        // handled RejectedExecutionException. The failure propagates to the caller.
        throwNext.set(true);
        assertThrows(RuntimeException.class, delegationTokenManager::reobtainDelegationTokens);

        // The scheduler is healthy again. The next re-obtain must schedule a fresh obtain
        // cycle instead of being coalesced against the cycle that never got scheduled.
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(
                1,
                delegate.getActiveScheduledTasks().size(),
                "A re-obtain after a scheduler failure must schedule a fresh obtain cycle");
        assertThat(onlyScheduledDelayMillis(delegate))
                .as("a failed scheduling attempt must not start the cooldown")
                .isZero();
    }

    @ParameterizedTest(name = "configured={0}ms, submission retry={1}ms")
    @CsvSource({"0, 1000", "100, 1000", "2500, 2500"})
    public void submissionRetryDelayMustRespectMinimumAndConfiguredBackoff(
            long configuredBackoffMillis, long expectedRetryMillis) throws Exception {
        try (SchedulingRejectionTestContext context =
                new SchedulingRejectionTestContext(
                        false, Duration.ofMillis(configuredBackoffMillis))) {
            context.manager.start(tokens -> {});
            context.rejectScheduling = true;
            context.manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(context.retryExecutor))
                    .isEqualTo(expectedRetryMillis);

            context.clock.advanceTime(Duration.ofMillis(expectedRetryMillis));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            assertThat(onlyScheduledDelayMillis(context.retryExecutor))
                    .isEqualTo(expectedRetryMillis);

            context.rejectScheduling = false;
            context.clock.advanceTime(Duration.ofMillis(expectedRetryMillis));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            context.rejectIoExecution = true;
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(expectedRetryMillis);

            context.clock.advanceTime(Duration.ofMillis(expectedRetryMillis));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(expectedRetryMillis);

            context.rejectIoExecution = false;
            context.clock.advanceTime(Duration.ofMillis(expectedRetryMillis));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.ioExecutor.triggerAll();
            assertThat(context.obtains).hasValue(2);
        }
    }

    @Test
    public void submissionRejectionsMustShareRateLimitedWarnings() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(false)) {
            context.manager.start(tokens -> {});
            context.manager.reobtainDelegationTokens();
            context.rejectIoExecution = true;
            context.rejectScheduling = true;
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();

            final List<LogEvent> initialRejections = submissionRejectionEvents();
            assertThat(initialRejections)
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG);
            assertThat(initialRejections.get(0).getThrown())
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(initialRejections.get(1).getThrown()).isNull();

            context.clock.advanceTime(Duration.ofMillis(59_999L));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            assertThat(submissionRejectionEvents())
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG, Level.DEBUG);

            context.clock.advanceTime(Duration.ofMillis(1L));
            context.manager.maybeScheduleRenewal(0L);
            final List<LogEvent> rejections = submissionRejectionEvents();
            assertThat(rejections)
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG, Level.DEBUG, Level.WARN);
            assertThat(rejections.subList(1, rejections.size()))
                    .allSatisfy(event -> assertThat(event.getThrown()).isNull());
            assertThat(rejections.get(3).getMessage().getFormattedMessage()).contains("4 times");
        }
    }

    @ParameterizedTest(name = "restart={0}")
    @ValueSource(booleans = {false, true})
    public void successfulCycleOrRestartMustResetSubmissionRejectionWarnings(boolean restart)
            throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(false)) {
            context.manager.start(tokens -> {});
            context.rejectIoExecution = true;
            context.manager.reobtainDelegationTokens();
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();

            // Accepting a retry timer is not recovery while the IO executor still rejects it.
            assertThat(submissionRejectionEvents())
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG);

            context.rejectIoExecution = false;
            if (restart) {
                context.manager.stop();
                context.manager.start(tokens -> {});
                context.scheduledExecutor.triggerScheduledTasks();
            } else {
                context.clock.advanceTime(
                        Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
                context.scheduledExecutor.triggerNonPeriodicScheduledTask();
                context.ioExecutor.triggerAll();
            }
            assertThat(context.obtains).hasValue(2);

            context.rejectIoExecution = true;
            context.manager.reobtainDelegationTokens();
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(submissionRejectionEvents())
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG, Level.WARN);
            assertThat(submissionRejectionEvents().get(2).getThrown())
                    .isInstanceOf(RejectedExecutionException.class);

            context.clock.advanceTime(Duration.ofMinutes(1));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            final List<LogEvent> rejections = submissionRejectionEvents();
            assertThat(rejections)
                    .extracting(LogEvent::getLevel)
                    .containsExactly(Level.WARN, Level.DEBUG, Level.WARN, Level.WARN);
            assertThat(rejections.get(3).getThrown()).isNull();
            assertThat(rejections.get(3).getMessage().getFormattedMessage()).contains("2 times");
        }
    }

    private List<LogEvent> submissionRejectionEvents() {
        return loggerAuditingExtension.getEvents().stream()
                .filter(
                        event ->
                                event.getMessage()
                                        .getFormattedMessage()
                                        .startsWith("Token update submission"))
                .collect(Collectors.toList());
    }

    @ParameterizedTest(name = "periodic={0}")
    @ValueSource(booleans = {false, true})
    public void schedulerRejectionMustRetryWithoutAnotherRequest(boolean periodic)
            throws Exception {
        try (SchedulingRejectionTestContext context =
                new SchedulingRejectionTestContext(periodic)) {
            context.rejectScheduling = true;
            context.manager.start(tokens -> {});
            if (!periodic) {
                context.manager.reobtainDelegationTokens();
            }

            assertThat(context.obtains).hasValue(1);
            assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
            final long retryDelay =
                    periodic
                            ? SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS
                            : SchedulingRejectionTestContext.RETRY_DELAY_MILLIS;
            assertThat(onlyScheduledDelayMillis(context.retryExecutor)).isEqualTo(retryDelay);

            context.rejectScheduling = false;
            context.clock.advanceTime(Duration.ofMillis(retryDelay));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor)).isZero();
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.ioExecutor.triggerAll();

            assertThat(context.obtains).hasValue(2);
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
            if (periodic) {
                assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                        .isEqualTo(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS);
            } else {
                assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
            }
        }
    }

    @Test
    public void rejectedOnDemandReplacementMustRetainRenewalProgress() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(true)) {
            context.manager.start(tokens -> {});
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS);

            context.rejectScheduling = true;
            context.manager.reobtainDelegationTokens();
            assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(onlyScheduledDelayMillis(context.retryExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS);

            context.rejectScheduling = false;
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            context.scheduledExecutor.triggerScheduledTasks();
            context.ioExecutor.triggerAll();

            assertThat(context.obtains).hasValue(2);
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS);
        }
    }

    @Test
    public void ioRejectionMustRetryWithoutAnotherRequest() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(true)) {
            context.manager.start(tokens -> {});
            context.rejectIoExecution = true;
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();

            assertThat(context.obtains).hasValue(1);
            assertThat(context.ioExecutor.numQueuedRunnables()).isZero();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS);

            context.rejectIoExecution = false;
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.ioExecutor.triggerAll();

            assertThat(context.obtains).hasValue(2);
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS);
        }
    }

    @Test
    public void staleIoRejectionMustNotReplaceNewerCycle() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(true)) {
            context.manager.start(tokens -> {});
            context.rejectIoExecution = true;
            context.beforeIoRejection = () -> context.manager.maybeScheduleRenewal(123L);
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS));

            // Replace the cycle after dispatch begins but before the rejection is handled.
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor)).isEqualTo(123L);
            assertThat(context.obtains).hasValue(1);

            context.beforeIoRejection = () -> {};
            context.rejectIoExecution = false;
            context.clock.advanceTime(Duration.ofMillis(123L));
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.ioExecutor.triggerAll();

            assertThat(context.obtains).hasValue(2);
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(onlyScheduledDelayMillis(context.scheduledExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RENEWAL_DELAY_MILLIS);
        }
    }

    @Test
    public void ioShutdownMustNotScheduleAnotherRetry() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(true)) {
            context.manager.start(tokens -> {});
            context.ioShutdown = true;
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();

            assertThat(context.obtains).hasValue(1);
            assertThat(context.ioExecutor.numQueuedRunnables()).isZero();
            assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
        }
    }

    @Test
    public void repeatedSchedulerRejectionsMustKeepRequestsCoalesced() throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(false)) {
            context.manager.start(tokens -> {});
            context.rejectScheduling = true;
            context.manager.reobtainDelegationTokens();
            context.manager.reobtainDelegationTokens();
            assertThat(context.schedulingRetries).hasSize(1);

            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            context.manager.reobtainDelegationTokens();
            context.manager.reobtainDelegationTokens();

            assertThat(context.schedulingRetries).hasSize(2);
            assertThat(onlyScheduledDelayMillis(context.retryExecutor))
                    .isEqualTo(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS);
            assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(context.obtains).hasValue(1);

            context.rejectScheduling = false;
            context.clock.advanceTime(
                    Duration.ofMillis(SchedulingRejectionTestContext.RETRY_DELAY_MILLIS));
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            context.scheduledExecutor.triggerNonPeriodicScheduledTask();
            context.ioExecutor.triggerAll();

            assertThat(context.obtains).hasValue(2);
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
        }
    }

    @ParameterizedTest(name = "restart={0}")
    @ValueSource(booleans = {false, true})
    public void stoppedSessionSchedulingRetryMustNotSubmitWork(boolean restart) throws Exception {
        try (SchedulingRejectionTestContext context = new SchedulingRejectionTestContext(false)) {
            context.manager.start(tokens -> {});
            context.rejectScheduling = true;
            context.manager.reobtainDelegationTokens();
            assertThat(context.schedulingRetries).hasSize(1);

            context.manager.stop();
            assertThat(context.schedulingRetries.get(0)).isCancelled();
            context.rejectScheduling = false;
            if (restart) {
                context.manager.start(tokens -> {});
                context.manager.reobtainDelegationTokens();
            }

            // The delayed completion can still run after its logical retry was cancelled.
            context.retryExecutor.triggerNonPeriodicScheduledTask();
            assertThat(context.retryExecutor.getActiveScheduledTasks()).isEmpty();
            assertThat(context.ioExecutor.numQueuedRunnables()).isZero();
            if (restart) {
                assertThat(context.scheduledExecutor.getAllScheduledTasks()).hasSize(1);
                context.manager.reobtainDelegationTokens();
                assertThat(context.scheduledExecutor.getAllScheduledTasks()).hasSize(1);
                context.scheduledExecutor.triggerNonPeriodicScheduledTask();
                context.ioExecutor.triggerAll();
                assertThat(context.obtains).hasValue(3);
            } else {
                assertThat(context.scheduledExecutor.getActiveScheduledTasks()).isEmpty();
                assertThat(context.obtains).hasValue(1);
            }
        }
    }

    @Test
    public void stopShouldKeepProvidersUsableForSubsequentStart() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        new Configuration(), null, scheduledExecutor, scheduler);

        // The manager is a process-lifetime singleton reused across ResourceManager leadership
        // sessions: stop() runs on every leadership revoke and start() on the next grant, with
        // the same provider instances. Providers are init()-ed exactly once, in the manager
        // constructor, and the SPI has no re-init hook, so a provider closed by stop() stays
        // broken for every following term. Providers must therefore only be closed at genuine
        // process shutdown, not by the per-session stop().
        delegationTokenManager.start(tokens -> {});
        delegationTokenManager.stop();
        delegationTokenManager.start(tokens -> {});

        assertFalse(
                ExceptionThrowingDelegationTokenProvider.closed.get(),
                "A leadership-session stop() must not close the providers, the next start()"
                        + " re-uses them");
    }

    @Test
    public void startShouldBeIdempotent() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        // The throw provider produces tokens so the listener gets notified on every cycle.
        ExceptionThrowingDelegationTokenProvider.addToken.set(true);
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), true);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);

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

        AtomicInteger firstListenerNotifications = new AtomicInteger(0);
        AtomicInteger secondListenerNotifications = new AtomicInteger(0);
        delegationTokenManager.start(tokens -> firstListenerNotifications.incrementAndGet());
        // A redundant start() (e.g. a buggy caller) must be ignored: no second inline obtain
        // cycle, and the listener of the running manager must not be swapped.
        delegationTokenManager.start(tokens -> secondListenerNotifications.incrementAndGet());

        assertEquals(
                1,
                startTokensUpdateCallCount.get(),
                "A redundant start() must not run another obtain cycle");
        assertEquals(
                1,
                firstListenerNotifications.get(),
                "The first start()'s inline cycle must notify the listener once");

        // A later cycle must still notify the original listener, not the ignored one.
        delegationTokenManager.reobtainDelegationTokens();
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();
        assertEquals(
                2,
                firstListenerNotifications.get(),
                "The original listener must keep receiving tokens");
        assertEquals(
                0,
                secondListenerNotifications.get(),
                "The listener from the ignored start() must never receive tokens");
    }

    @Test
    public void retryMustBringPendingOnDemandReobtainForward() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler, clock);

        delegationTokenManager.start(tokens -> {});

        delegationTokenManager.reobtainDelegationTokens();
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s later a second re-obtain is cooldown-deferred by 50s.
        clock.advanceTime(Duration.ofMillis(10_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(50_000L, onlyScheduledDelayMillis(scheduledExecutor));

        // A failed cycle now wants a retry in 10s. The pending on-demand cycle must be brought
        // forward to the sooner time instead of silently swallowing the retry, otherwise the
        // effective retry would fire 50s out while the backoff (and its token-TTL cap) asked
        // for 10s.
        delegationTokenManager.maybeScheduleRenewal(10_000L);
        assertEquals(
                10_000L,
                onlyScheduledDelayMillis(scheduledExecutor),
                "A sooner retry must bring the pending on-demand cycle forward");
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
    public void waitingReobtainMustKeepFurtherRequestsCoalesced() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final List<Thread> ioThreads = new CopyOnWriteArrayList<>();
        final ThreadPoolExecutor ioExecutor =
                (ThreadPoolExecutor)
                        Executors.newFixedThreadPool(
                                3,
                                runnable -> {
                                    final Thread thread = new Thread(runnable);
                                    ioThreads.add(thread);
                                    return thread;
                                });
        final ManualClock clock = new ManualClock();
        final OneShotLatch blockedObtain = new OneShotLatch();
        final OneShotLatch releaseObtain = new OneShotLatch();
        final OneShotLatch pendingObtain = new OneShotLatch();
        final OneShotLatch subsequentObtain = new OneShotLatch();
        final AtomicInteger obtainCalls = new AtomicInteger();
        final DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMinutes(1)),
                        null,
                        scheduledExecutor,
                        ioExecutor,
                        clock) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        final int obtainCall = obtainCalls.incrementAndGet();
                        if (obtainCall == 2) {
                            blockedObtain.trigger();
                            releaseObtain.awaitQuietly();
                        } else if (obtainCall == 3) {
                            pendingObtain.trigger();
                        } else if (obtainCall == 4) {
                            subsequentObtain.trigger();
                        }
                        return Optional.empty();
                    }
                };

        try {
            delegationTokenManager.start(tokens -> {});
            delegationTokenManager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            blockedObtain.await();

            clock.advanceTime(Duration.ofMinutes(1));
            delegationTokenManager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            // With the first obtain parked, this state identifies the second worker waiting
            // for renewalCycleLock. Requests arriving now must remain covered by that worker.
            CommonTestUtils.waitUntilCondition(
                    () -> ioThreads.get(1).getState() == Thread.State.BLOCKED);

            for (int request = 0; request < 5; request++) {
                clock.advanceTime(Duration.ofMinutes(1));
                delegationTokenManager.reobtainDelegationTokens();
                scheduledExecutor.triggerScheduledTasks();
            }

            assertThat(ioExecutor.getTaskCount()).isEqualTo(2);
            assertThat(ioExecutor.getQueue()).isEmpty();
            assertThat(ioExecutor.submit(() -> "unrelated IO completed").get())
                    .isEqualTo("unrelated IO completed");
            assertThat(obtainCalls).hasValue(2);

            releaseObtain.trigger();
            pendingObtain.await();
            assertThat(obtainCalls).hasValue(3);

            clock.advanceTime(Duration.ofMinutes(1));
            delegationTokenManager.reobtainDelegationTokens();
            assertThat(scheduledExecutor.getActiveScheduledTasks()).hasSize(1);
            scheduledExecutor.triggerScheduledTasks();
            subsequentObtain.await();
            assertThat(obtainCalls).hasValue(4);
        } finally {
            releaseObtain.trigger();
            delegationTokenManager.close();
            ioExecutor.shutdownNow();
            for (Thread thread : ioThreads) {
                thread.join();
            }
        }
    }

    @ParameterizedTest(name = "restart={0}")
    @ValueSource(booleans = {false, true})
    public void waitingCycleMustNotObtainAfterSessionEnds(boolean restart) throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final OneShotLatch initialObtain = new OneShotLatch();
        final OneShotLatch releaseObtain = new OneShotLatch();
        final AtomicInteger obtainCalls = new AtomicInteger();
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ZERO),
                        null,
                        scheduledExecutor,
                        ioExecutor) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        if (obtainCalls.incrementAndGet() == 1) {
                            initialObtain.trigger();
                            releaseObtain.awaitQuietly();
                        }
                        return Optional.empty();
                    }
                };
        final CheckedThread initialStart =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        manager.start(tokens -> {});
                    }
                };
        final CheckedThread waitingCycle =
                new CheckedThread() {
                    @Override
                    public void go() {
                        ioExecutor.trigger();
                    }
                };
        final CheckedThread nextStart =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        manager.start(tokens -> {});
                    }
                };

        try {
            initialStart.start();
            initialObtain.await();
            manager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);
            waitingCycle.start();
            CommonTestUtils.waitUntilCondition(
                    () -> waitingCycle.getState() == Thread.State.BLOCKED);

            manager.stop();
            if (restart) {
                nextStart.start();
                // start() publishes the new epoch before waiting for the previous obtain.
                CommonTestUtils.waitUntilCondition(
                        () -> nextStart.getState() == Thread.State.BLOCKED);
            }

            releaseObtain.trigger();
            initialStart.sync();
            waitingCycle.sync();
            if (restart) {
                nextStart.sync();
            }
            assertThat(obtainCalls)
                    .as("only each session's initial cycle obtains; the old waiting cycle skips")
                    .hasValue(restart ? 2 : 1);
        } finally {
            releaseObtain.trigger();
            manager.close();
            for (CheckedThread thread :
                    new CheckedThread[] {initialStart, waitingCycle, nextStart}) {
                if (thread.getState() != Thread.State.NEW) {
                    thread.sync();
                }
            }
        }
    }

    @Test
    public void renewalCycleLockSerializesConcurrentObtainCycles() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ExecutorService ioExecutor = Executors.newFixedThreadPool(2);
        try {
            // The barrier trips only if two obtain cycles are inside the obtain/broadcast
            // section at the same time. renewalCycleLock must serialize them, so each should
            // time out.
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean concurrentObtainDetected = new AtomicBoolean(false);
            final CountDownLatch done = new CountDownLatch(2);
            // Enabled only after start(): its inline first cycle must not wait at (and, by
            // timing out, break) the barrier meant for the two concurrent cycles below.
            final AtomicBoolean barrierEnabled = new AtomicBoolean(false);

            DefaultDelegationTokenManager delegationTokenManager =
                    new DefaultDelegationTokenManager(
                            new Configuration(), null, scheduledExecutor, ioExecutor) {
                        @Override
                        protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                                DelegationTokenContainer container) {
                            if (!barrierEnabled.get()) {
                                return Optional.empty();
                            }
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

            delegationTokenManager.start(tokens -> {});
            barrierEnabled.set(true);

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
                    "renewalCycleLock must prevent two obtain cycles from running concurrently");
        } finally {
            ioExecutor.shutdownNow();
        }
    }

    @Test
    public void failedReregistrationMustNotWipePreviousRegistration() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        // The job registers successfully when it starts...
        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        assertEquals(1, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());

        // ...and later re-registers while still running (e.g. after a JobMaster<->RM heartbeat
        // timeout). A transient provider failure during the re-registration must not roll back
        // the per-job state the earlier successful registration established, or obtain cycles
        // would broadcast token sets missing the running job until a registration retry
        // succeeds.
        ExceptionThrowingDelegationTokenProvider.throwInRegister.set(true);
        assertThrows(
                IllegalArgumentException.class,
                () -> delegationTokenManager.registerJob(jobId, new Configuration()));

        assertEquals(
                1,
                ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size(),
                "A failed re-registration must keep the previous registration intact");
    }

    @Test
    public void stopShouldUnregisterAllRegisteredJobs() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        delegationTokenManager.registerJob(JobID.generate(), new Configuration());
        delegationTokenManager.registerJob(JobID.generate(), new Configuration());
        assertEquals(2, ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size());

        // stop() ends the manager's (leadership) session. Jobs still running re-register with
        // the next session (registerJob is idempotent by contract), while jobs that reached a
        // terminal state when no session was active never re-register, and their per-job provider
        // state must be released here or it leaks for the process lifetime.
        delegationTokenManager.stop();

        assertEquals(
                0,
                ExceptionThrowingDelegationTokenProvider.registeredJobs.get().size(),
                "stop() must release the per-job provider state of every registered job");
    }

    @Test
    public void failedRegistrationIsNotTrackedUntilARetrySucceeds() throws Exception {
        final DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);
        final JobID jobId = JobID.generate();

        // Registration stores job state and then fails. Rollback also fails,
        // leaving the state in the provider.
        ExceptionThrowingDelegationTokenProvider.throwErrorInRegister.set(true);
        ExceptionThrowingDelegationTokenProvider.throwInUnregister.set(true);
        assertThatThrownBy(() -> delegationTokenManager.registerJob(jobId, new Configuration()))
                .isInstanceOf(NoClassDefFoundError.class);
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .containsExactly(jobId);

        // Allow cleanup to succeed, but keep registration failing. No successful registration
        // should be tracked, so the next failed attempt must roll back the leftover state.
        ExceptionThrowingDelegationTokenProvider.throwInUnregister.set(false);
        assertThatThrownBy(() -> delegationTokenManager.registerJob(jobId, new Configuration()))
                .isInstanceOf(NoClassDefFoundError.class);
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .as("a failed registration retry removes leftover provider state")
                .isEmpty();

        // Recovery must track the successful retry so session shutdown cleans it up.
        ExceptionThrowingDelegationTokenProvider.throwErrorInRegister.set(false);
        delegationTokenManager.registerJob(jobId, new Configuration());
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .containsExactly(jobId);

        delegationTokenManager.stop();
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .as("stop() releases the successfully registered job")
                .isEmpty();
    }

    @Test
    public void failedUnregistrationMustDropTheJob() throws Exception {
        final DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        final JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, new Configuration());
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .containsExactly(jobId);

        // Cleanup fails and leaves provider state behind, but the manager must
        // stop tracking the job.
        ExceptionThrowingDelegationTokenProvider.throwInUnregister.set(true);
        delegationTokenManager.unregisterJob(jobId);
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .containsExactly(jobId);

        // Allow cleanup to succeed. stop() must not retry the failed unregistration.
        ExceptionThrowingDelegationTokenProvider.throwInUnregister.set(false);
        delegationTokenManager.stop();
        assertThat(ExceptionThrowingDelegationTokenProvider.registeredJobs.get())
                .as("stop() must not retry a failed unregistration")
                .containsExactly(jobId);
    }

    @Test
    public void cooldownMustSpaceObtainCycleExecutionsNotRequests() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler,
                        clock);

        delegationTokenManager.start(tokens -> {});

        // The first request runs immediately.
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // A request at t0+1s is deferred by 59s: its obtain cycle runs at t0+60s.
        clock.advanceTime(Duration.ofMillis(1_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(59_000L, onlyScheduledDelayMillis(scheduledExecutor));
        clock.advanceTime(Duration.ofMillis(59_000L));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // The option documents a minimum time between two consecutive on-demand obtain CYCLES,
        // so a request arriving just after the deferred cycle ran must be deferred by a full
        // cooldown measured from that cycle's execution, not run (almost) immediately because
        // the previous REQUEST arrived one cooldown ago.
        clock.advanceTime(Duration.ofMillis(1_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(
                59_000L,
                onlyScheduledDelayMillis(scheduledExecutor),
                "The cooldown must space obtain cycle executions, not requests");
    }

    @Test
    public void broughtForwardReobtainMustMoveCooldownAnchor() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        long t0 = 1_000_000L;
        ManualClock clock = new ManualClock(t0 * 1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler,
                        clock);

        delegationTokenManager.start(tokens -> {});

        delegationTokenManager.reobtainDelegationTokens();
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s later a second request is cooldown-deferred by 50s (would run at t0+60s).
        clock.advanceTime(Duration.ofMillis(10_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(50_000L, onlyScheduledDelayMillis(scheduledExecutor));

        // A completed cycle (renewal or failure retry) brings the pending on-demand cycle
        // forward to +5s, so the coalesced cycle actually executes at t0+15s.
        delegationTokenManager.maybeScheduleRenewal(5_000L);
        assertEquals(5_000L, onlyScheduledDelayMillis(scheduledExecutor));
        clock.advanceTime(Duration.ofMillis(5_000L));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // The next request must measure its cooldown from the brought-forward execution
        // (t0+15s), not from the originally scheduled t0+60s. Otherwise it would defer
        // beyond a full cooldown (104s here instead of 59s).
        clock.advanceTime(Duration.ofMillis(1_000L));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(
                59_000L,
                onlyScheduledDelayMillis(scheduledExecutor),
                "The cooldown anchor must follow a brought-forward on-demand cycle");
    }

    @Test
    public void failedBringForwardMustPreservePreviousCooldownAnchor() throws Exception {
        final AtomicBoolean throwNext = new AtomicBoolean();
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor() {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        if (throwNext.compareAndSet(true, false)) {
                            throw new IllegalStateException("simulated scheduler failure");
                        }
                        return super.schedule(command, delay, unit);
                    }
                };
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final ManualClock clock = new ManualClock();
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMinutes(1)),
                        null,
                        scheduledExecutor,
                        ioExecutor,
                        clock);
        try {
            manager.start(tokens -> {});
            manager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            ioExecutor.triggerAll();

            clock.advanceTime(Duration.ofSeconds(10));
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(50_000L);
            throwNext.set(true);
            assertThatThrownBy(() -> manager.maybeScheduleRenewal(5_000L))
                    .isInstanceOf(IllegalStateException.class);

            clock.advanceTime(Duration.ofSeconds(1));
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                    .as("failed rescheduling must preserve the last actual cycle start at t=0")
                    .isEqualTo(49_000L);
        } finally {
            manager.close();
        }
    }

    @ParameterizedTest(name = "previous cycle failed: {0}")
    @ValueSource(booleans = {false, true})
    public void queuedRenewalServingDemandMustAnchorCooldownAtCycleStart(
            boolean previousCycleFailed) throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final ManualClock clock = new ManualClock();
        final AtomicInteger obtains = new AtomicInteger();
        final Configuration configuration = hermeticCooldownConfig(Duration.ofMinutes(1));
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 1.0);
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, ioExecutor, clock) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        if (obtains.incrementAndGet() == 2) {
                            if (previousCycleFailed) {
                                throw new IllegalStateException("simulated obtain failure");
                            }
                            return Optional.of(clock.absoluteTimeMillis() + 10_000L);
                        }
                        return Optional.of(clock.absoluteTimeMillis() + 300_000L);
                    }

                    @Override
                    long calculateRetryDelay(Clock ignored) {
                        return 10_000L;
                    }
                };
        try {
            manager.start(tokens -> {});
            manager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            ioExecutor.triggerAll();
            assertThat(obtains).hasValue(2);
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(10_000L);

            // Queue the periodic/retry worker before demand schedules a cycle for t=60s.
            clock.advanceTime(Duration.ofSeconds(10));
            scheduledExecutor.triggerScheduledTasks();
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(50_000L);
            ioExecutor.triggerAll();
            assertThat(obtains).hasValue(3);
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(300_000L);

            clock.advanceTime(Duration.ofSeconds(1));
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                    .as("the queued worker served demand at t=10s, not the planned t=60s")
                    .isEqualTo(59_000L);
        } finally {
            manager.close();
        }
    }

    @Test
    public void delayedReobtainMustAnchorCooldownAtCycleStart() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final ManualClock clock = new ManualClock();
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMinutes(1)),
                        null,
                        scheduledExecutor,
                        ioExecutor,
                        clock);
        try {
            manager.start(tokens -> {});
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isZero();
            scheduledExecutor.triggerScheduledTasks();

            // The timer fired immediately, but the IO worker cannot start for two minutes.
            clock.advanceTime(Duration.ofMinutes(2));
            ioExecutor.triggerAll();
            clock.advanceTime(Duration.ofSeconds(1));
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                    .as("the cooldown starts when the IO worker begins the obtain cycle")
                    .isEqualTo(59_000L);
        } finally {
            manager.close();
        }
    }

    @Test
    public void reobtainDuringObtainMustUseActualCycleStart() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final ManualClock clock = new ManualClock();
        final AtomicInteger obtains = new AtomicInteger();
        final Configuration configuration = hermeticCooldownConfig(Duration.ofMinutes(1));
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 1.0);
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, ioExecutor, clock) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        if (obtains.incrementAndGet() == 2) {
                            clock.advanceTime(Duration.ofSeconds(10));
                            reobtainDelegationTokens();
                            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                                    .as("demand during obtain uses the cycle start at t=120s")
                                    .isEqualTo(50_000L);
                        }
                        return Optional.of(clock.absoluteTimeMillis() + 300_000L);
                    }
                };
        try {
            manager.start(tokens -> {});
            manager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();

            clock.advanceTime(Duration.ofMinutes(2));
            ioExecutor.triggerAll();
            assertThat(obtains).hasValue(2);
            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                    .as("successful obtain must preserve the earlier pending demand")
                    .isEqualTo(50_000L);

            clock.advanceTime(Duration.ofSeconds(50));
            scheduledExecutor.triggerScheduledTasks();
            ioExecutor.triggerAll();
            assertThat(obtains).hasValue(3);
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(300_000L);
        } finally {
            manager.close();
        }
    }

    @Test
    public void periodicRenewalWithoutDemandMustNotMoveCooldownAnchor() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final ManualClock clock = new ManualClock();
        final AtomicInteger obtains = new AtomicInteger();
        final Configuration configuration = hermeticCooldownConfig(Duration.ofMinutes(1));
        configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 1.0);
        final DefaultDelegationTokenManager manager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, ioExecutor, clock) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        final long renewalDelay =
                                obtains.incrementAndGet() == 2 ? 10_000L : 300_000L;
                        return Optional.of(clock.absoluteTimeMillis() + renewalDelay);
                    }
                };
        try {
            manager.start(tokens -> {});
            manager.reobtainDelegationTokens();
            scheduledExecutor.triggerScheduledTasks();
            ioExecutor.triggerAll();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor)).isEqualTo(10_000L);

            clock.advanceTime(Duration.ofSeconds(10));
            scheduledExecutor.triggerScheduledTasks();
            ioExecutor.triggerAll();
            assertThat(obtains).hasValue(3);

            clock.advanceTime(Duration.ofSeconds(1));
            manager.reobtainDelegationTokens();
            assertThat(onlyScheduledDelayMillis(scheduledExecutor))
                    .as("ordinary periodic renewal must preserve the on-demand anchor at t=0")
                    .isEqualTo(49_000L);
        } finally {
            manager.close();
        }
    }

    @Test
    public void startAfterStopMustResetRetryState() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        Configuration configuration = hermeticCooldownConfig(Duration.ofMillis(60_000));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, Duration.ofSeconds(1));
        configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF, Duration.ofSeconds(64));
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler);

        delegationTokenManager.start(tokens -> {});
        // The session escalates the retry state through repeated obtain failures.
        delegationTokenManager.currentRetryBackoff = Duration.ofSeconds(64).toMillis();
        delegationTokenManager.lastKnownNextRenewal = 123L;

        // The manager instance is reused across leadership sessions: the next session must
        // start from the configured initial backoff instead of inheriting the previous
        // session's increased one (which would delay token recovery by up to the max backoff).
        delegationTokenManager.stop();
        delegationTokenManager.start(tokens -> {});

        assertEquals(
                Duration.ofSeconds(1).toMillis(),
                delegationTokenManager.currentRetryBackoff,
                "A new session must start from the initial retry backoff");
        assertEquals(
                Long.MAX_VALUE,
                delegationTokenManager.lastKnownNextRenewal,
                "A new session must not inherit the previous session's renewal deadline");
    }

    @Test
    public void inFlightCycleMustNotNotifyListenerAfterStop() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        // The "throw" provider stays enabled so its RECEIVER is loaded: the token added below
        // must have a live receiver, or a regression that removes the broadcast gate would hide
        // behind the missing-receiver IllegalStateException (swallowed by the cycle's catch) and
        // this test would pass vacuously. The overridden obtain below bypasses the providers, so
        // the throw provider itself never runs.
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);

        final CountDownLatch cycleInObtain = new CountDownLatch(1);
        final CountDownLatch resumeObtain = new CountDownLatch(1);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        // Produce a token so the broadcast path is reached, then park until the
                        // test has run stop(), simulating slow provider I/O overlapping shutdown.
                        container.addToken("throw", new byte[] {1});
                        cycleInObtain.countDown();
                        try {
                            resumeObtain.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return Optional.empty();
                    }
                };

        AtomicInteger listenerNotifications = new AtomicInteger(0);
        // start() runs the first obtain cycle inline, so it parks in the obtain above.
        Thread starter =
                new Thread(
                        () -> {
                            try {
                                delegationTokenManager.start(
                                        tokens -> listenerNotifications.incrementAndGet());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        starter.start();
        assertTrue(cycleInObtain.await(10, TimeUnit.SECONDS));

        // stop() does not wait for the in-flight cycle. Once that cycle resumes it must notice
        // the manager stopped: the stopped session's listener must not be notified, and the
        // manager must not keep referencing it (it is the disposed ResourceManager).
        delegationTokenManager.stop();
        resumeObtain.countDown();
        starter.join(10_000L);
        assertFalse(starter.isAlive());

        assertEquals(
                0,
                listenerNotifications.get(),
                "An obtain cycle finishing after stop() must not notify the stopped session's"
                        + " listener");
        assertNull(delegationTokenManager.listener, "stop() must release the listener reference");
    }

    @Test
    public void inFlightCycleMustNotDeliverIntoTheNextSession() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        // Same setup rationale as inFlightCycleMustNotNotifyListenerAfterStop: keep the "throw"
        // receiver loaded so the broadcast path is real. Unlike there, only the first obtain
        // parks (the old session's inline cycle). The new session's first cycle must run
        // through.
        Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hadoopfs.enabled"), false);
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".hbase.enabled"), false);

        final CountDownLatch cycleInObtain = new CountDownLatch(1);
        final CountDownLatch resumeObtain = new CountDownLatch(1);
        final AtomicBoolean parkNextObtain = new AtomicBoolean(true);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        configuration, null, scheduledExecutor, scheduler) {
                    @Override
                    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                            DelegationTokenContainer container) {
                        container.addToken("throw", new byte[] {1});
                        if (parkNextObtain.compareAndSet(true, false)) {
                            cycleInObtain.countDown();
                            try {
                                // Longer than the readiness-poll deadline below, so A cannot
                                // resume on its own while the test is still waiting for B.
                                resumeObtain.await(30, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        return Optional.empty();
                    }
                };

        AtomicReference<Throwable> starterFailure = new AtomicReference<>();
        AtomicInteger sessionANotifications = new AtomicInteger(0);
        Thread starterA =
                new Thread(
                        () -> {
                            try {
                                delegationTokenManager.start(
                                        tokens -> sessionANotifications.incrementAndGet());
                            } catch (Throwable t) {
                                starterFailure.compareAndSet(null, t);
                            }
                        });
        starterA.start();
        assertTrue(cycleInObtain.await(10, TimeUnit.SECONDS));

        // Leadership changes while A's cycle is parked in the obtain: stop() ends session A and
        // the next session starts before the cycle resumes. start(B) publishes B's listener
        // under schedulingLock and then blocks on renewalCycleLock behind A's cycle, so the
        // resuming cycle observes running == true and B's listener.
        delegationTokenManager.stop();
        AtomicInteger sessionBNotifications = new AtomicInteger(0);
        Thread starterB =
                new Thread(
                        () -> {
                            try {
                                delegationTokenManager.start(
                                        tokens -> sessionBNotifications.incrementAndGet());
                            } catch (Throwable t) {
                                starterFailure.compareAndSet(null, t);
                            }
                        });
        starterB.start();
        // Wait until start(B) is parked on renewalCycleLock behind A's cycle. B publishes its
        // listener under the manager's lock strictly before it can block there, so a durable
        // BLOCKED state implies the listener is in place without reading the lock-guarded
        // field unsynchronized. This gates when to resume A's parked cycle, so A's re-check
        // exercises the epoch fence, not the running flag.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (starterB.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(Thread.State.BLOCKED, starterB.getState());

        resumeObtain.countDown();
        starterA.join(10_000L);
        starterB.join(10_000L);
        assertFalse(starterA.isAlive());
        assertFalse(starterB.isAlive());
        if (starterFailure.get() != null) {
            throw new AssertionError("start() failed in a session thread", starterFailure.get());
        }

        assertEquals(
                0,
                sessionANotifications.get(),
                "Session A's listener was released by stop() and must not be notified");
        assertEquals(
                1,
                sessionBNotifications.get(),
                "A cycle that began under an earlier session must not deliver into the next"
                        + " session: only the next session's own first cycle may notify it");
    }

    @Test
    public void cooldownMustBeImmuneToWallClockJumps() throws Exception {
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();

        JumpableClock clock = new JumpableClock(1_000_000L);
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(
                        hermeticCooldownConfig(Duration.ofMillis(60_000)),
                        null,
                        scheduledExecutor,
                        scheduler,
                        clock);

        delegationTokenManager.start(tokens -> {});
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(0L, onlyScheduledDelayMillis(scheduledExecutor));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s of real time pass, then NTP steps the wall clock back by an hour. The cooldown is
        // a process-local interval, so the next request must still be deferred by the remaining
        // 50s, not by the wall-clock difference.
        clock.advance(Duration.ofSeconds(10));
        clock.jumpWallClock(Duration.ofHours(-1));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(
                50_000L,
                onlyScheduledDelayMillis(scheduledExecutor),
                "The cooldown must be computed on monotonic time: a wall-clock rollback must"
                        + " not extend it");

        // Let the deferred cycle run at its due time (the anchor sits at its execution time).
        clock.advance(Duration.ofSeconds(50));
        scheduledExecutor.triggerScheduledTasks();
        scheduler.triggerAll();

        // 10s later the wall clock jumps two hours forward. Under wall-clock math that would
        // zero the remaining cooldown. The monotonic cooldown must still defer by 50s.
        clock.advance(Duration.ofSeconds(10));
        clock.jumpWallClock(Duration.ofHours(2));
        delegationTokenManager.reobtainDelegationTokens();
        assertEquals(
                50_000L,
                onlyScheduledDelayMillis(scheduledExecutor),
                "The cooldown must be computed on monotonic time: a wall-clock jump forward"
                        + " must not bypass it");
    }

    @Test
    public void registerJobMustNotExposeCallersConfigurationToProviders() throws Exception {
        DefaultDelegationTokenManager delegationTokenManager =
                new DefaultDelegationTokenManager(new Configuration(), null, null, null);

        // The caller's configuration object is the live job configuration (the ExecutionPlan's),
        // which reaches the manager by reference over local RPC. Providers are plugin code and
        // must receive a copy: a provider mutating it must not corrupt the runtime's state.
        ExceptionThrowingDelegationTokenProvider.mutateJobConfiguration.set(true);
        Configuration callerConfiguration = new Configuration();
        JobID jobId = JobID.generate();
        delegationTokenManager.registerJob(jobId, callerConfiguration);

        assertTrue(ExceptionThrowingDelegationTokenProvider.registeredJobs.get().contains(jobId));
        assertFalse(
                callerConfiguration.containsKey(
                        ExceptionThrowingDelegationTokenProvider.MUTATED_KEY),
                "A provider-side mutation must not be visible in the caller's configuration");
    }

    private static final class SchedulingRejectionTestContext implements AutoCloseable {
        private static final long RETRY_DELAY_MILLIS = 1000L;
        private static final long RENEWAL_DELAY_MILLIS = 10_000L;

        private final ManualClock clock = new ManualClock();
        private final AtomicInteger obtains = new AtomicInteger();
        private final List<CompletableFuture<Void>> schedulingRetries = new ArrayList<>();
        private final ManuallyTriggeredScheduledExecutor retryExecutor =
                new ManuallyTriggeredScheduledExecutor();
        private boolean rejectScheduling;
        private boolean rejectIoExecution;
        private boolean ioShutdown;
        private Runnable beforeIoRejection = () -> {};
        private final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor() {
                    @Override
                    public ScheduledFuture<?> schedule(
                            Runnable command, long delay, TimeUnit unit) {
                        if (rejectScheduling) {
                            throw new RejectedExecutionException("scheduler is saturated");
                        }
                        return super.schedule(command, delay, unit);
                    }
                };
        private final ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService() {
                    @Override
                    public void execute(Runnable command) {
                        if (rejectIoExecution || ioShutdown) {
                            beforeIoRejection.run();
                            throw new RejectedExecutionException("IO executor is saturated");
                        }
                        super.execute(command);
                    }

                    @Override
                    public boolean isShutdown() {
                        return ioShutdown;
                    }
                };
        private final DefaultDelegationTokenManager manager;

        private SchedulingRejectionTestContext(boolean periodic) {
            this(periodic, Duration.ofMillis(RETRY_DELAY_MILLIS));
        }

        private SchedulingRejectionTestContext(boolean periodic, Duration retryBackoff) {
            final Configuration configuration = hermeticCooldownConfig(Duration.ZERO);
            configuration.set(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF, retryBackoff);
            configuration.set(DELEGATION_TOKENS_RENEWAL_TIME_RATIO, 1.0);
            manager =
                    new DefaultDelegationTokenManager(
                            configuration, null, scheduledExecutor, ioExecutor, clock) {
                        @Override
                        protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
                                DelegationTokenContainer container) {
                            obtains.incrementAndGet();
                            return periodic
                                    ? Optional.of(clock.absoluteTimeMillis() + RENEWAL_DELAY_MILLIS)
                                    : Optional.empty();
                        }

                        @Override
                        void completeSchedulingRetry(
                                CompletableFuture<Void> retryFuture, long delayMillis) {
                            schedulingRetries.add(retryFuture);
                            retryExecutor.schedule(
                                    () -> retryFuture.complete(null),
                                    delayMillis,
                                    TimeUnit.MILLISECONDS);
                        }
                    };
        }

        @Override
        public void close() {
            manager.close();
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

    /**
     * A clock whose absolute (wall) time can jump independently of its relative (monotonic) time,
     * simulating an NTP step or a manual clock adjustment. {@link ManualClock} cannot express this:
     * it drives both flavors from one counter.
     */
    private static final class JumpableClock extends Clock {
        private final AtomicLong absoluteMillis;
        private final AtomicLong relativeNanos = new AtomicLong();

        JumpableClock(long absoluteMillis) {
            this.absoluteMillis = new AtomicLong(absoluteMillis);
        }

        @Override
        public long absoluteTimeMillis() {
            return absoluteMillis.get();
        }

        @Override
        public long relativeTimeMillis() {
            return relativeNanos.get() / 1_000_000L;
        }

        @Override
        public long relativeTimeNanos() {
            return relativeNanos.get();
        }

        void advance(Duration duration) {
            absoluteMillis.addAndGet(duration.toMillis());
            relativeNanos.addAndGet(duration.toNanos());
        }

        /** Steps the wall clock only; relative time is unaffected, like a real NTP step. */
        void jumpWallClock(Duration duration) {
            absoluteMillis.addAndGet(duration.toMillis());
        }
    }
}
