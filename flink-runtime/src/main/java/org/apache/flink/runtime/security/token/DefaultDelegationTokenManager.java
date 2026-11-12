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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.plugin.PluginManager;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKEN_PROVIDER_ENABLED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation tokens are enabled, this manager will make sure long-running apps can run
 * without interruption while accessing secured services. It periodically contacts all the
 * configured secure services to obtain delegation tokens, updates the receivers of the local JVM
 * and hands the tokens to the attached listener to be distributed to the rest of the application.
 *
 * <p>Tokens are obtained for the lifetime of the process ({@link #start()} to {@link #stop()}) and
 * not for its leadership, so a standby JobManager keeps its local receivers fresh and does not take
 * over with aged tokens. A listener is attached for the duration of a leader session only.
 *
 * <p>The manager never renews a token, even where the service supports it. It obtains new tokens
 * before the old ones expire, so it works the same way for every service.
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

    private final Configuration configuration;

    private final Clock clock;

    @Nullable private final PluginManager pluginManager;

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryInitialBackoff;

    private final long renewalRetryMaxBackoff;

    /**
     * The backoff of the next retry of a failed obtain. It doubles with every retry up to the
     * maximum and is reset by a successful obtain.
     */
    @VisibleForTesting long currentRetryBackoff;

    /** The earliest expiry of the tokens of the last successful obtain. */
    @VisibleForTesting long lastKnownExpiry = Long.MAX_VALUE;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    private final DelegationTokenReceiverRepository delegationTokenReceiverRepository;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    private final Object stateLock = new Object();

    @GuardedBy("stateLock")
    private State state = State.CREATED;

    @GuardedBy("stateLock")
    @Nullable
    private ScheduledFuture<?> scheduledObtain;

    @GuardedBy("stateLock")
    @Nullable
    private Listener listener;

    /** The newest tokens in the form the listener receives them. */
    @GuardedBy("stateLock")
    @Nullable
    private byte[] latestTokens;

    /**
     * Serializes the listener callbacks so that a listener never sees older tokens after newer
     * ones, and lets {@link #detachListener(Listener)} wait for the one in progress. Never acquired
     * while holding {@link #stateLock}.
     */
    private final Object deliveryLock = new Object();

    public DefaultDelegationTokenManager(
            Configuration configuration,
            @Nullable PluginManager pluginManager,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this(
                configuration,
                pluginManager,
                scheduledExecutor,
                ioExecutor,
                Clock.systemDefaultZone());
    }

    @VisibleForTesting
    DefaultDelegationTokenManager(
            Configuration configuration,
            @Nullable PluginManager pluginManager,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor,
            Clock clock) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.pluginManager = pluginManager;
        this.tokensRenewalTimeRatio = configuration.get(DELEGATION_TOKENS_RENEWAL_TIME_RATIO);
        this.renewalRetryInitialBackoff =
                configuration.get(DELEGATION_TOKENS_RENEWAL_RETRY_INITIAL_BACKOFF).toMillis();
        this.renewalRetryMaxBackoff =
                configuration.get(DELEGATION_TOKENS_RENEWAL_RETRY_MAX_BACKOFF).toMillis();
        this.currentRetryBackoff = renewalRetryInitialBackoff;
        this.delegationTokenProviders = loadProviders();
        this.delegationTokenReceiverRepository =
                new DelegationTokenReceiverRepository(configuration, pluginManager);
        this.scheduledExecutor = scheduledExecutor;
        this.ioExecutor = ioExecutor;
        this.clock = checkNotNull(clock, "Clock must not be null");
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

        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        Consumer<DelegationTokenProvider> loadProvider =
                (provider) -> {
                    try {
                        if (isProviderEnabled(configuration, provider.serviceName())) {
                            provider.init(configuration);
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
        obtainDelegationTokensAndGetExpiry(container);
        LOG.info("Delegation tokens obtained successfully");
    }

    /**
     * Obtains the tokens of the providers which require them, adds them to the container and
     * returns their expiry: the earliest {@code validUntil} of the obtained tokens, empty if no
     * provider reports one.
     */
    protected Optional<Long> obtainDelegationTokensAndGetExpiry(
            DelegationTokenContainer container) {
        return delegationTokenProviders.values().stream()
                .map(
                        p -> {
                            Optional<Long> validUntil = Optional.empty();
                            try {
                                if (p.delegationTokensRequired()) {
                                    LOG.debug(
                                            "Obtaining delegation token for service {}",
                                            p.serviceName());
                                    DelegationTokenProvider.ObtainedDelegationTokens t =
                                            p.obtainDelegationTokens();
                                    checkNotNull(t, "Obtained delegation tokens must not be null");
                                    container.addToken(p.serviceName(), t.getTokens());
                                    validUntil = t.getValidUntil();
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
                            return validUntil;
                        })
                .flatMap(validUntil -> validUntil.map(Stream::of).orElseGet(Stream::empty))
                .min(Long::compare);
    }

    /**
     * Starts obtaining tokens for the lifetime of the process: obtains tokens inline, propagates
     * them to the local JVM receivers and schedules the next obtain. A failure of the first obtain
     * is not retried but thrown, because the process may need the tokens right away (e.g.
     * BlobServer).
     */
    @Override
    public void start() throws Exception {
        checkNotNull(scheduledExecutor, "Scheduled executor must not be null");
        checkNotNull(ioExecutor, "IO executor must not be null");
        synchronized (stateLock) {
            checkState(
                    state == State.CREATED,
                    "The delegation token manager can only be started once, current state: %s",
                    state);
            state = State.RUNNING;
        }

        try {
            obtainDistributeAndScheduleNext();
        } catch (Throwable t) {
            stop();
            throw t;
        }
    }

    @Override
    public void attachListener(Listener listener) throws Exception {
        checkNotNull(listener, "Listener must not be null");
        synchronized (stateLock) {
            checkState(
                    state == State.RUNNING,
                    "The delegation token manager must be running to attach a listener, current state: %s",
                    state);
            if (this.listener != null && this.listener != listener) {
                LOG.warn("Replacing the attached delegation token listener");
            }
            this.listener = listener;
            LOG.info("Delegation token listener attached");
        }
        deliverLatestTokens();
    }

    @Override
    public void detachListener(Listener listener) {
        checkNotNull(listener, "Listener must not be null");
        // Waits for the delivery in progress, so that no callback runs once this method returns.
        synchronized (deliveryLock) {
            synchronized (stateLock) {
                if (this.listener == listener) {
                    this.listener = null;
                    LOG.info("Delegation token listener detached");
                }
            }
        }
    }

    /**
     * One obtain: gets new tokens from the providers, updates the local JVM receivers, stores the
     * tokens as the latest ones, delivers them to the attached listener and schedules the next
     * obtain.
     */
    private void obtainDistributeAndScheduleNext() throws Exception {
        LOG.info("Obtaining and distributing delegation tokens");
        DelegationTokenContainer container = new DelegationTokenContainer();
        Optional<Long> expiry = obtainDelegationTokensAndGetExpiry(container);

        if (container.hasTokens()) {
            delegationTokenReceiverRepository.onNewTokensObtained(container);
            storeAndDeliverLatestTokens(InstantiationUtil.serializeObject(container));
        } else {
            LOG.warn("No tokens obtained so skipping the update of the receivers and the delivery");
        }

        if (expiry.isPresent()) {
            lastKnownExpiry = expiry.get();
            currentRetryBackoff = renewalRetryInitialBackoff;
            long obtainDelay = calculateObtainDelay(clock, expiry.get());
            if (scheduleNextObtainIfRunning(obtainDelay)) {
                LOG.info(
                        "Next delegation token obtain scheduled with {} delay",
                        TimeUtils.formatWithHighestUnit(Duration.ofMillis(obtainDelay)));
            }
        } else {
            LOG.warn(
                    "Next delegation token obtain not scheduled because either no tokens were obtained or none of the tokens specified its expiry");
        }
    }

    private void storeAndDeliverLatestTokens(byte[] serializedTokens) throws Exception {
        synchronized (stateLock) {
            latestTokens = serializedTokens;
        }
        deliverLatestTokens();
    }

    /**
     * Hands the newest tokens to the attached listener. Every delivery reads them while it holds
     * the delivery lock, so a delivery which was waiting for another one does not hand over tokens
     * which are older than the ones the other one handed over.
     */
    private void deliverLatestTokens() throws Exception {
        synchronized (deliveryLock) {
            final Listener currentListener;
            final byte[] tokens;
            synchronized (stateLock) {
                currentListener = listener;
                tokens = latestTokens;
            }
            if (currentListener != null && tokens != null) {
                LOG.info("Delivering the latest tokens to the listener");
                currentListener.onNewTokensObtained(tokens);
                LOG.info("Latest tokens delivered to the listener successfully");
            }
        }
    }

    /** Returns whether the next obtain was scheduled, it is not if the manager is not running. */
    private boolean scheduleNextObtainIfRunning(long delayMillis) {
        synchronized (stateLock) {
            if (state != State.RUNNING) {
                LOG.info(
                        "Next delegation token obtain is not scheduled, the manager is not running");
                return false;
            }
            scheduledObtain =
                    scheduledExecutor.schedule(
                            () -> ioExecutor.execute(this::runScheduledObtainAndRetryOnFailure),
                            delayMillis,
                            TimeUnit.MILLISECONDS);
            return true;
        }
    }

    private void runScheduledObtainAndRetryOnFailure() {
        try {
            obtainDistributeAndScheduleNext();
        } catch (InterruptedException e) {
            // Ignore, may happen if shutting down.
            LOG.debug("Interrupted", e);
        } catch (Exception e) {
            long delay = calculateNextRetryDelay(clock);
            scheduleNextObtainIfRunning(delay);
            LOG.warn(
                    "Failed to obtain and distribute tokens, will try again in {}",
                    TimeUtils.formatWithHighestUnit(Duration.ofMillis(delay)),
                    e);
        }
    }

    private void cancelScheduledObtain() {
        synchronized (stateLock) {
            if (scheduledObtain != null) {
                scheduledObtain.cancel(true);
                scheduledObtain = null;
            }
        }
    }

    /**
     * Returns the delay of the next retry of a failed obtain and advances the backoff for the retry
     * after it.
     */
    @VisibleForTesting
    long calculateNextRetryDelay(Clock clock) {
        long nowMillis = clock.millis();
        long effectiveMax;
        if (lastKnownExpiry != Long.MAX_VALUE) {
            long remaining = lastKnownExpiry - nowMillis;
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
    long calculateObtainDelay(Clock clock, long expiry) {
        long now = clock.millis();
        long obtainDelay = Math.round(tokensRenewalTimeRatio * (expiry - now));
        LOG.debug(
                "Calculated obtain delay is {}, based on expiry {}, the ratio {} and the current time {}",
                obtainDelay,
                expiry,
                tokensRenewalTimeRatio,
                now);
        return obtainDelay;
    }

    /** Stops obtaining tokens and releases the attached listener. */
    @Override
    public void stop() {
        LOG.info("Stopping to obtain delegation tokens");

        synchronized (stateLock) {
            state = State.STOPPED;
            listener = null;
        }
        cancelScheduledObtain();

        LOG.info("Stopped obtaining delegation tokens");
    }

    /** Lifecycle of the manager, tokens are obtained while it is running. */
    private enum State {
        CREATED,
        RUNNING,
        STOPPED
    }
}
