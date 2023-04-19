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
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_RETRY_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKENS_RENEWAL_TIME_RATIO;
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

    private final Configuration configuration;

    @Nullable private final PluginManager pluginManager;

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryBackoffPeriod;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    private final DelegationTokenReceiverRepository delegationTokenReceiverRepository;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    @Nullable
    private ScheduledFuture<?> tokensUpdateFuture;

    @Nullable private Listener listener;

    public DefaultDelegationTokenManager(
            Configuration configuration,
            @Nullable PluginManager pluginManager,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.pluginManager = pluginManager;
        this.tokensRenewalTimeRatio = configuration.get(DELEGATION_TOKENS_RENEWAL_TIME_RATIO);
        this.renewalRetryBackoffPeriod =
                configuration.get(DELEGATION_TOKENS_RENEWAL_RETRY_BACKOFF).toMillis();
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
                .getBoolean(DELEGATION_TOKEN_PROVIDER_ENABLED);
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
        this.listener = checkNotNull(listener, "Listener must not be null");
        synchronized (tokensUpdateFutureLock) {
            checkState(tokensUpdateFuture == null, "Manager is already started");
        }

        startTokensUpdate();
    }

    @VisibleForTesting
    void startTokensUpdate() {
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
                long renewalDelay =
                        calculateRenewalDelay(Clock.systemDefaultZone(), nextRenewal.get());
                synchronized (tokensUpdateFutureLock) {
                    tokensUpdateFuture =
                            scheduledExecutor.schedule(
                                    () -> ioExecutor.execute(this::startTokensUpdate),
                                    renewalDelay,
                                    TimeUnit.MILLISECONDS);
                }
                LOG.info("Tokens update task started with {} ms delay", renewalDelay);
            } else {
                LOG.warn(
                        "Tokens update task not started because either no tokens obtained or none of the tokens specified its renewal date");
            }
        } catch (InterruptedException e) {
            // Ignore, may happen if shutting down.
            LOG.debug("Interrupted", e);
        } catch (Exception e) {
            synchronized (tokensUpdateFutureLock) {
                tokensUpdateFuture =
                        scheduledExecutor.schedule(
                                () -> ioExecutor.execute(this::startTokensUpdate),
                                renewalRetryBackoffPeriod,
                                TimeUnit.MILLISECONDS);
            }
            LOG.warn(
                    "Failed to update tokens, will try again in {} ms",
                    renewalRetryBackoffPeriod,
                    e);
        }
    }

    @VisibleForTesting
    void stopTokensUpdate() {
        synchronized (tokensUpdateFutureLock) {
            if (tokensUpdateFuture != null) {
                tokensUpdateFuture.cancel(true);
                tokensUpdateFuture = null;
            }
        }
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

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping credential renewal");

        stopTokensUpdate();

        LOG.info("Stopped credential renewal");
    }
}
