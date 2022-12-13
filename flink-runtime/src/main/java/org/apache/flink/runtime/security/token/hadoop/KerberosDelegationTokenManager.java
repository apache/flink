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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.token.DelegationTokenContainer;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.security.token.DelegationTokenProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_TOKENS_RENEWAL_RETRY_BACKOFF;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_TOKENS_RENEWAL_TIME_RATIO;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It periodically logs in to the KDC
 * with user-provided credentials, and contacts all the configured secure services to obtain
 * delegation tokens to be distributed to the rest of the application.
 */
@Internal
public class KerberosDelegationTokenManager implements DelegationTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosDelegationTokenManager.class);

    private final Configuration configuration;

    private final double tokensRenewalTimeRatio;

    private final long renewalRetryBackoffPeriod;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    private final Object tokensUpdateFutureLock = new Object();

    @GuardedBy("tokensUpdateFutureLock")
    @Nullable
    private ScheduledFuture<?> tokensUpdateFuture;

    @Nullable private Listener listener;

    public KerberosDelegationTokenManager(
            Configuration configuration,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.tokensRenewalTimeRatio = configuration.get(KERBEROS_TOKENS_RENEWAL_TIME_RATIO);
        this.renewalRetryBackoffPeriod =
                configuration.get(KERBEROS_TOKENS_RENEWAL_RETRY_BACKOFF).toMillis();
        this.delegationTokenProviders = loadProviders();
        this.scheduledExecutor = scheduledExecutor;
        this.ioExecutor = ioExecutor;
    }

    private Map<String, DelegationTokenProvider> loadProviders() {
        LOG.info("Loading delegation token providers");

        ServiceLoader<DelegationTokenProvider> serviceLoader =
                ServiceLoader.load(DelegationTokenProvider.class);

        Map<String, DelegationTokenProvider> providers = new HashMap<>();
        for (DelegationTokenProvider provider : serviceLoader) {
            try {
                if (isProviderEnabled(provider.serviceName())) {
                    provider.init(configuration);
                    LOG.info(
                            "Delegation token provider {} loaded and initialized",
                            provider.serviceName());
                    checkState(
                            !providers.containsKey(provider.serviceName()),
                            "Delegation token provider with service name {} has multiple implementations",
                            provider.serviceName());
                    providers.put(provider.serviceName(), provider);
                } else {
                    LOG.info(
                            "Delegation token provider {} is disabled so not loaded",
                            provider.serviceName());
                }
            } catch (Exception | NoClassDefFoundError e) {
                LOG.error(
                        "Failed to initialize delegation token provider {}",
                        provider.serviceName(),
                        e);
                if (!(e instanceof NoClassDefFoundError)) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }

        LOG.info("Delegation token providers loaded successfully");

        return providers;
    }

    @VisibleForTesting
    boolean isProviderEnabled(String serviceName) {
        return configuration.getBoolean(
                String.format("security.kerberos.token.provider.%s.enabled", serviceName), true);
    }

    @VisibleForTesting
    boolean isProviderLoaded(String serviceName) {
        return delegationTokenProviders.containsKey(serviceName);
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
                byte[] containerBytes = InstantiationUtil.serializeObject(container);
                HadoopDelegationTokenUpdater.addCurrentUserCredentials(containerBytes);

                LOG.info("Notifying listener about new tokens");
                checkNotNull(listener, "Listener must not be null");
                listener.onNewTokensObtained(containerBytes);
                LOG.info("Listener notified successfully");
            } else {
                LOG.warn("No tokens obtained so skipping listener notification");
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
