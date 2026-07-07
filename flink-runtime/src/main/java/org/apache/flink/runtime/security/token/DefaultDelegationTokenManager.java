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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.security.token.DelegationTokenManagerCallback;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_REOBTAIN_COOLDOWN;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKEN_PROVIDER_ENABLED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It periodically contacts all the
 * configured secure services to obtain delegation tokens to be distributed to the rest of the
 * application.
 */
@Internal
public class DefaultDelegationTokenManager implements DelegationTokenManager {

    private static final String PROVIDER_RECEIVER_INCONSISTENCY_ERROR =
            "There is an inconsistency between loaded delegation token providers and receivers. "
                    + "One must implement a DelegationTokenProvider and a DelegationTokenReceiver "
                    + "with the same service name and add them together to the classpath to make "
                    + "the system consistent. The mentioned classes are loaded with Java's service "
                    + "loader so the appropriate META-INF registration also needs to be created.";

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDelegationTokenManager.class);

    private static final long NO_PREVIOUS_REOBTAIN = Long.MIN_VALUE;

    private final Configuration configuration;

    @Nullable private final PluginManager pluginManager;

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryInitialBackoff;

    private final long renewalRetryMaxBackoff;

    @VisibleForTesting long currentRetryBackoff;

    @VisibleForTesting long lastKnownNextRenewal = Long.MAX_VALUE;

    private final long reobtainCooldownMillis;

    /** Clock used for cooldown bookkeeping; overridable in tests. */
    private volatile Clock clock = Clock.systemDefaultZone();

    /**
     * Serializes the obtain-and-broadcast cycle so that, even though {@code cancel(true)} does not
     * wait for an in-flight cycle and the IO executor is multi-threaded, two cycles can never run
     * concurrently and broadcast tokens out of order.
     */
    private final Object obtainLock = new Object();

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    private final DelegationTokenReceiverRepository delegationTokenReceiverRepository;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    @Nullable
    private ScheduledFuture<?> tokensUpdateFuture;

    /**
     * Clock time (millis) at which {@link #tokensUpdateFuture} is scheduled to fire, or {@link
     * Long#MAX_VALUE} when no cycle is pending. Lets an on-demand re-obtain only ever bring the
     * next obtain cycle <em>forward</em> and never push an already-scheduled (e.g. periodic)
     * renewal later, which could otherwise let a short-lived token expire before it is renewed.
     */
    @GuardedBy("tokensUpdateFutureLock")
    private long nextScheduledAtMillis = Long.MAX_VALUE;

    /** Whether an on-demand re-obtain is scheduled but has not started executing yet (dedupe). */
    @GuardedBy("tokensUpdateFutureLock")
    private boolean reobtainScheduled;

    /**
     * Clock time (millis) of the last on-demand re-obtain request, used to enforce the cooldown.
     * Holds {@link #NO_PREVIOUS_REOBTAIN} until the first request. Only on-demand re-obtains update
     * this (not the periodic renewal), so the cooldown spaces requests, not obtain executions.
     */
    @GuardedBy("tokensUpdateFutureLock")
    private long lastReobtainAtMillis = NO_PREVIOUS_REOBTAIN;

    /**
     * Whether the manager is between {@link #start(Listener)} and {@link #stop()}. Defaults to
     * false, so work arriving before the first start() is rejected the same way as after stop().
     */
    @GuardedBy("tokensUpdateFutureLock")
    private boolean running;

    @Nullable private Listener listener;

    public DefaultDelegationTokenManager(
            Configuration configuration,
            @Nullable PluginManager pluginManager,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.pluginManager = pluginManager;
        this.tokensRenewalTimeRatio = configuration.get(DELEGATION_TOKENS_RENEWAL_TIME_RATIO);
        this.renewalRetryInitialBackoff =
                configuration.get(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF).toMillis();
        this.renewalRetryMaxBackoff =
                configuration.get(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF).toMillis();
        this.currentRetryBackoff = renewalRetryInitialBackoff;
        this.reobtainCooldownMillis =
                configuration.get(DELEGATION_TOKENS_REOBTAIN_COOLDOWN).toMillis();
        this.delegationTokenProviders = loadProviders();
        this.delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, pluginManager);
        this.scheduledExecutor = scheduledExecutor;
        this.ioExecutor = ioExecutor;
        checkProviderAndReceiverConsistency(
                delegationTokenProviders,
                delegationTokenReceiverRepository.delegationTokenReceivers);
        Set<String> warnings = new HashSet<>();
        checkSamePrefixedProviders(delegationTokenProviders, warnings);
        for (String warning : warnings) {
            LOG.warn(warning);
        }
    }

    private Map<String, DelegationTokenProvider> loadProviders() {
        LOG.info("Loading delegation token providers");

        // Handed to every provider so it can request an immediate re-obtain later, from any
        // thread, decoupled from the registerJob call stack.
        final DelegationTokenManagerCallback callback = this::reobtainDelegationTokens;
        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        Consumer<DelegationTokenProvider> loadProvider =
                (provider) -> {
                    try {
                        if (isProviderEnabled(configuration, provider.serviceName())) {
                            provider.init(configuration, callback);
                            LOG.info(
                                    "Delegation token provider {} loaded and initialized",
                                    provider.serviceName());
                            checkState(
                                    !providers.containsKey(provider.serviceName()),
                                    "Delegation token provider with service name "
                                            + provider.serviceName()
                                            + " has multiple implementations");
                            providers.put(provider.serviceName(), provider);
                        } else {
                            LOG.info(
                                    "Delegation token provider {} is disabled so not loaded",
                                    provider.serviceName());
                        }
                    } catch (Exception | NoClassDefFoundError e) {
                        // The intentional general rule is that if a provider's init method throws
                        // exception
                        // then stop the workload
                        LOG.error(
                                "Failed to initialize delegation token provider {}",
                                provider.serviceName(),
                                e);
                        throw new FlinkRuntimeException(e);
                    }
                };
        ServiceLoader.load(DelegationTokenProvider.class).iterator().forEachRemaining(loadProvider);
        if (pluginManager != null) {
            pluginManager.load(DelegationTokenProvider.class).forEachRemaining(loadProvider);
        }

        LOG.info("Delegation token providers loaded successfully");

        return providers;
    }

    static boolean isProviderEnabled(Configuration configuration, String serviceName) {
        return SecurityOptions.forProvider(configuration, serviceName)
                .get(DELEGATION_TOKEN_PROVIDER_ENABLED);
    }

    @VisibleForTesting
    boolean isProviderLoaded(String serviceName) {
        return delegationTokenProviders.containsKey(serviceName);
    }

    @VisibleForTesting
    boolean isReceiverLoaded(String serviceName) {
        return delegationTokenReceiverRepository.isReceiverLoaded(serviceName);
    }

    @VisibleForTesting
    static void checkProviderAndReceiverConsistency(
            Map<String, DelegationTokenProvider> providers,
            Map<String, DelegationTokenReceiver> receivers) {
        LOG.info("Checking provider and receiver instances consistency");
        if (providers.size() != receivers.size()) {
            Set<String> missingReceiverServiceNames = new HashSet<>(providers.keySet());
            missingReceiverServiceNames.removeAll(receivers.keySet());
            if (!missingReceiverServiceNames.isEmpty()) {
                throw new IllegalStateException(
                        PROVIDER_RECEIVER_INCONSISTENCY_ERROR
                                + " Missing receivers: "
                                + String.join(",", missingReceiverServiceNames));
            }

            Set<String> missingProviderServiceNames = new HashSet<>(receivers.keySet());
            missingProviderServiceNames.removeAll(providers.keySet());
            if (!missingProviderServiceNames.isEmpty()) {
                throw new IllegalStateException(
                        PROVIDER_RECEIVER_INCONSISTENCY_ERROR
                                + " Missing providers: "
                                + String.join(",", missingProviderServiceNames));
            }
        }
        LOG.info("Provider and receiver instances are consistent");
    }

    @VisibleForTesting
    static void checkSamePrefixedProviders(
            Map<String, DelegationTokenProvider> providers, Set<String> warnings) {
        Set<String> providerPrefixes = new HashSet<>();
        for (String name : providers.keySet()) {
            String[] split = name.split("-");
            if (!providerPrefixes.add(split[0])) {
                String msg =
                        String.format(
                                "Multiple providers loaded with the same prefix: %s. This might lead to unintended consequences, please consider using only one of them.",
                                split[0]);
                warnings.add(msg);
            }
        }
    }

    /**
     * Obtains new tokens in a one-time fashion and leaves it up to the caller to distribute them.
     */
    @Override
    public void obtainDelegationTokens(DelegationTokenContainer container) throws Exception {
        LOG.info("Obtaining delegation tokens");
        obtainDelegationTokensAndGetNextRenewal(container);
        LOG.info("Delegation tokens obtained successfully");
    }

    @Override
    public void obtainDelegationTokens() throws Exception {
        LOG.info("Obtaining delegation tokens");
        DelegationTokenContainer container = new DelegationTokenContainer();
        obtainDelegationTokensAndGetNextRenewal(container);
        LOG.info("Delegation tokens obtained successfully");

        if (container.hasTokens()) {
            delegationTokenReceiverRepository.onNewTokensObtained(container);
        } else {
            LOG.warn("No tokens obtained so skipping notifications");
        }
    }

    protected Optional<Long> obtainDelegationTokensAndGetNextRenewal(
            DelegationTokenContainer container) {
        return delegationTokenProviders.values().stream()
                .map(
                        p -> {
                            Optional<Long> nr = Optional.empty();
                            try {
                                if (p.delegationTokensRequired()) {
                                    LOG.debug(
                                            "Obtaining delegation token for service {}",
                                            p.serviceName());
                                    DelegationTokenProvider.ObtainedDelegationTokens t =
                                            p.obtainDelegationTokens();
                                    checkNotNull(t, "Obtained delegation tokens must not be null");
                                    container.addToken(p.serviceName(), t.getTokens());
                                    nr = t.getValidUntil();
                                    LOG.debug(
                                            "Obtained delegation token for service {} successfully",
                                            p.serviceName());
                                } else {
                                    LOG.debug(
                                            "Service {} does not need to obtain delegation token",
                                            p.serviceName());
                                }
                            } catch (Exception e) {
                                LOG.error(
                                        "Failed to obtain delegation token for provider {}",
                                        p.serviceName(),
                                        e);
                                throw new FlinkRuntimeException(e);
                            }
                            return nr;
                        })
                .flatMap(nr -> nr.map(Stream::of).orElseGet(Stream::empty))
                .min(Long::compare);
    }

    /**
     * Creates a re-occurring task which obtains new tokens and automatically distributes them to
     * task managers.
     */
    @Override
    public void start(Listener listener) throws Exception {
        checkNotNull(scheduledExecutor, "Scheduled executor must not be null");
        checkNotNull(ioExecutor, "IO executor must not be null");
        checkNotNull(listener, "Listener must not be null");
        synchronized (tokensUpdateFutureLock) {
            if (running) {
                LOG.warn("DelegationTokenManager is already started, ignoring redundant start()");
                return;
            }
            this.listener = listener;
            // Set before the inline first cycle below: startTokensUpdate() and
            // maybeScheduleRenewal() gate on it.
            running = true;
        }

        startTokensUpdate();
    }

    @VisibleForTesting
    void startTokensUpdate() {
        synchronized (tokensUpdateFutureLock) {
            // Clear the dedupe flag so later on-demand requests can schedule a fresh cycle.
            reobtainScheduled = false;
            // Stopped or never started: skip the cycle. The providers may already be stopped
            // and the listener may not be set yet.
            if (!running) {
                return;
            }
        }
        // Serialize the obtain-and-broadcast so a re-obtain racing the periodic renewal cannot run
        // two cycles concurrently on the (multi-threaded) IO executor and broadcast out of order.
        synchronized (obtainLock) {
            try {
                LOG.info("Starting tokens update task");
                DelegationTokenContainer container = new DelegationTokenContainer();
                Optional<Long> nextRenewal = obtainDelegationTokensAndGetNextRenewal(container);

                if (container.hasTokens()) {
                    delegationTokenReceiverRepository.onNewTokensObtained(container);

                    LOG.info("Notifying listener about new tokens");
                    checkNotNull(listener, "Listener must not be null");
                    listener.onNewTokensObtained(InstantiationUtil.serializeObject(container));
                    LOG.info("Listener notified successfully");
                } else {
                    LOG.warn("No tokens obtained so skipping notifications");
                }

                if (nextRenewal.isPresent()) {
                    lastKnownNextRenewal = nextRenewal.get();
                    currentRetryBackoff = renewalRetryInitialBackoff;
                    long renewalDelay = calculateRenewalDelay(clock, nextRenewal.get());
                    long effectiveDelay = maybeScheduleRenewal(renewalDelay);
                    if (effectiveDelay >= 0) {
                        LOG.info(
                                "Tokens update task started with {} delay",
                                TimeUtils.formatWithHighestUnit(Duration.ofMillis(effectiveDelay)));
                    } else {
                        LOG.info("Tokens update task not rescheduled, the manager is not running");
                    }
                } else {
                    LOG.warn(
                            "Tokens update task not started because either no tokens obtained or none of the tokens specified its renewal date");
                }
            } catch (InterruptedException e) {
                // Ignore, may happen if shutting down.
                LOG.debug("Interrupted", e);
            } catch (Exception e) {
                long delay = calculateRetryDelay(clock);
                long effectiveDelay;
                try {
                    effectiveDelay = maybeScheduleRenewal(delay);
                } catch (Throwable schedulingFailure) {
                    // The original failure was not logged yet, keep it attached.
                    schedulingFailure.addSuppressed(e);
                    throw schedulingFailure;
                }
                if (effectiveDelay >= 0) {
                    LOG.warn(
                            "Failed to update tokens, will try again in {}",
                            TimeUtils.formatWithHighestUnit(Duration.ofMillis(effectiveDelay)),
                            e);
                } else {
                    LOG.warn(
                            "Failed to update tokens, no retry scheduled because the manager is "
                                    + "not running",
                            e);
                }
            }
        }
    }

    /**
     * Schedules a one-shot token-obtain-and-broadcast cycle after {@code delayMs}, replacing any
     * pending renewal. A delay of {@code 0} brings the next cycle forward to now. Must only be
     * called after {@link #start(Listener)} (the scheduled and IO executors are non-null then) and
     * while holding {@link #tokensUpdateFutureLock}.
     */
    @GuardedBy("tokensUpdateFutureLock")
    private void scheduleRenewalLocked(long delayMs) {
        stopTokensUpdate();
        nextScheduledAtMillis = clock.millis() + delayMs;
        try {
            tokensUpdateFuture =
                    scheduledExecutor.schedule(
                            () -> {
                                try {
                                    ioExecutor.execute(this::startTokensUpdate);
                                } catch (RejectedExecutionException e) {
                                    // IO executor is shutting down: drop the cycle but release the
                                    // dedupe flag so it cannot get stuck if the manager is reused.
                                    synchronized (tokensUpdateFutureLock) {
                                        reobtainScheduled = false;
                                    }
                                    LOG.debug("Tokens update task rejected by IO executor", e);
                                }
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduled executor is shutting down: no cycle will run, so undo the bookkeeping
            // this method set.
            reobtainScheduled = false;
            nextScheduledAtMillis = Long.MAX_VALUE;
            LOG.debug("Tokens update task rejected by scheduled executor", e);
        } catch (Throwable t) {
            // Undo the same bookkeeping as the rejection branch, or every later re-obtain would
            // be coalesced against a cycle that never got scheduled. Rethrow to keep the failure
            // visible.
            reobtainScheduled = false;
            nextScheduledAtMillis = Long.MAX_VALUE;
            throw t;
        }
    }

    /**
     * Schedules the next cycle (periodic renewal or failure retry). A pending on-demand cycle is
     * brought forward when {@code delayMs} is sooner and left in place otherwise, so a pending
     * cycle is never delayed.
     *
     * @param delayMs requested delay in millis
     * @return the delay in millis until the cycle that will actually run next, or -1 when nothing
     *     is scheduled because the manager is not running.
     */
    @VisibleForTesting
    long maybeScheduleRenewal(long delayMs) {
        // A negative delay (the token already passed its validUntil) means run now. Clamp it so
        // it cannot be mistaken for the -1 not-running sentinel.
        delayMs = Math.max(0L, delayMs);
        synchronized (tokensUpdateFutureLock) {
            if (!running) {
                return -1L;
            }
            if (reobtainScheduled) {
                long pendingInMillis = Math.max(0L, nextScheduledAtMillis - clock.millis());
                if (delayMs < pendingInMillis) {
                    // Bring the pending on-demand cycle forward. scheduleRenewalLocked() leaves
                    // reobtainScheduled set, so coalescing still holds and the earlier cycle
                    // serves the coalesced on-demand requests too.
                    scheduleRenewalLocked(delayMs);
                    return delayMs;
                }
                LOG.debug(
                        "An on-demand re-obtain is already scheduled to fire sooner, leaving it "
                                + "in place.");
                return pendingInMillis;
            }
            scheduleRenewalLocked(delayMs);
            return delayMs;
        }
    }

    @VisibleForTesting
    void stopTokensUpdate() {
        synchronized (tokensUpdateFutureLock) {
            if (tokensUpdateFuture != null) {
                tokensUpdateFuture.cancel(true);
                tokensUpdateFuture = null;
                nextScheduledAtMillis = Long.MAX_VALUE;
            }
        }
    }

    @VisibleForTesting
    long calculateRetryDelay(Clock clock) {
        long nowMillis = clock.millis();
        long effectiveMax;
        if (lastKnownNextRenewal != Long.MAX_VALUE) {
            long remaining = lastKnownNextRenewal - nowMillis;
            // If a failure occurs close to token expiry there may not be enough time for the
            // normal exponential backoff to complete before the token becomes invalid. Capping
            // each retry delay to one third of the remaining valid window ensures a retry always
            // happens while the token is still live, regardless of how close to expiry the
            // failure occurred.
            effectiveMax = remaining > 0 ? remaining / 3 : renewalRetryMaxBackoff;
        } else {
            effectiveMax = renewalRetryMaxBackoff;
        }
        long base = Math.min(currentRetryBackoff, effectiveMax);
        long jitter = (long) ((ThreadLocalRandom.current().nextDouble() - 0.5) * base);
        long delay = Math.max(0, Math.min(base + jitter, effectiveMax));
        currentRetryBackoff = Math.min(currentRetryBackoff * 2, renewalRetryMaxBackoff);
        return delay;
    }

    @VisibleForTesting
    long calculateRenewalDelay(Clock clock, long nextRenewal) {
        long now = clock.millis();
        long renewalDelay = Math.round(tokensRenewalTimeRatio * (nextRenewal - now));
        LOG.debug(
                "Calculated delay on renewal is {}, based on next renewal {} and the ratio {}, and current time {}",
                renewalDelay,
                nextRenewal,
                tokensRenewalTimeRatio,
                now);
        return renewalDelay;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping credential renewal");

        synchronized (tokensUpdateFutureLock) {
            // Mark not running, cancel the pending cycle, and reset the re-obtain bookkeeping
            // atomically, so a re-obtain racing shutdown cannot schedule a cycle for a manager
            // that is shutting down.
            running = false;
            stopTokensUpdate();
            reobtainScheduled = false;
            lastReobtainAtMillis = NO_PREVIOUS_REOBTAIN;
        }

        for (DelegationTokenProvider provider : delegationTokenProviders.values()) {
            try {
                provider.stop();
            } catch (Throwable t) {
                LOG.error("Failed to stop delegation token provider {}", provider.serviceName(), t);
            }
        }

        LOG.info("Stopped credential renewal");
    }

    @Override
    public void reobtainDelegationTokens() {
        synchronized (tokensUpdateFutureLock) {
            if (scheduledExecutor == null || ioExecutor == null) {
                LOG.debug(
                        "A re-obtain of delegation tokens was requested but the manager was "
                                + "constructed without executors (one-shot obtain path); the "
                                + "request is ignored.");
                return;
            }
            if (!running) {
                LOG.debug(
                        "A re-obtain of delegation tokens was requested while the manager is not "
                                + "running (not started yet, or already stopped), ignoring the "
                                + "request.");
                return;
            }
            // Dedupe: if an on-demand re-obtain is already scheduled and has not started yet, the
            // newly registered job(s) will be covered by it, so coalesce this request into it.
            if (reobtainScheduled) {
                LOG.debug("A re-obtain of delegation tokens is already scheduled; coalescing.");
                return;
            }
            // Cooldown: bound how often on-demand re-obtains can run by deferring this cycle until
            // at least reobtainCooldownMillis have passed since the previous on-demand re-obtain.
            long now = clock.millis();
            long delayMillis =
                    lastReobtainAtMillis == NO_PREVIOUS_REOBTAIN
                            ? 0L
                            : Math.max(0L, lastReobtainAtMillis + reobtainCooldownMillis - now);
            // Only bring the next cycle forward: if a cycle (e.g. the periodic renewal) is still
            // pending and scheduled to fire sooner than the cooldown-deferred time, fire at that
            // earlier time instead of pushing it later — otherwise a short-lived token could expire
            // before it is renewed. The nextScheduledAtMillis > now guard skips an already-fired
            // future that has not yet been rescheduled, so this never bypasses the cooldown.
            if (tokensUpdateFuture != null
                    && nextScheduledAtMillis > now
                    && nextScheduledAtMillis - now < delayMillis) {
                delayMillis = nextScheduledAtMillis - now;
            }
            lastReobtainAtMillis = now;
            reobtainScheduled = true;
            LOG.debug(
                    "Re-obtain of delegation tokens requested; scheduling an obtain cycle in {}",
                    TimeUtils.formatWithHighestUnit(Duration.ofMillis(delayMillis)));
            scheduleRenewalLocked(delayMillis);
        }
    }

    @Override
    public void registerJob(JobID jobId, Configuration jobConfiguration) throws Exception {
        try {
            for (DelegationTokenProvider provider : delegationTokenProviders.values()) {
                provider.registerJob(jobId, jobConfiguration);
            }
        } catch (Exception e) {
            // If any of the providers fail to register, then unregister the job from them all.
            // unregisterJob is idempotent, so it is safe to call it for providers that were never
            // (or only partially) registered for this job before the failure. The rollback must
            // never mask the original failure, so swallow any rollback exception.
            try {
                unregisterJob(jobId);
            } catch (Exception rollbackException) {
                LOG.error("Failed to roll back registration of job {}", jobId, rollbackException);
            }
            LOG.error("Failed to register job {}", jobId, e);
            throw e;
        }
    }

    @Override
    public void unregisterJob(JobID jobId) throws Exception {
        for (DelegationTokenProvider provider : delegationTokenProviders.values()) {
            try {
                provider.unregisterJob(jobId);
            } catch (Exception e) {
                LOG.error("Failed to unregister job for provider {}", provider.serviceName(), e);
            }
        }
    }
}
