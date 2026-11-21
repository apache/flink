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
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.configuration.SecurityOptions.DELEGATION_TOKEN_PROVIDER_ENABLED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Repository for delegation token providers. It loads the providers of the enabled services and
 * obtains the tokens from them.
 */
@Internal
public class DelegationTokenProviderRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(DelegationTokenProviderRepository.class);

    private final Configuration configuration;

    @Nullable private final PluginManager pluginManager;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    public DelegationTokenProviderRepository(
            Configuration configuration, @Nullable PluginManager pluginManager) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.pluginManager = pluginManager;
        this.delegationTokenProviders = loadProviders();
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
     * Obtains the tokens of the providers which require them, adds them to the container and
     * returns their expiry: the earliest {@code validUntil} of the obtained tokens, empty if no
     * provider reports one.
     */
    Optional<Long> obtainDelegationTokensAndGetExpiry(DelegationTokenContainer container) {
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
}
