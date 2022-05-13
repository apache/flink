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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It periodically logs in to the KDC
 * with user-provided credentials, and contacts all the configured secure services to obtain
 * delegation tokens to be distributed to the rest of the application.
 */
public class KerberosDelegationTokenManager implements DelegationTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosDelegationTokenManager.class);

    private final Configuration configuration;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    public KerberosDelegationTokenManager(Configuration configuration) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.delegationTokenProviders = loadProviders();
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
                    providers.put(provider.serviceName(), provider);
                } else {
                    LOG.info(
                            "Delegation token provider {} is disabled so not loaded",
                            provider.serviceName());
                }
            } catch (Exception e) {
                LOG.error(
                        "Failed to initialize delegation token provider {}.",
                        provider.serviceName(),
                        e);
                throw e;
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
    public void obtainDelegationTokens(Credentials credentials) {
        LOG.info("Obtaining delegation tokens");
    }

    /**
     * Creates a re-occurring task which obtains new tokens and automatically distributes them to
     * task managers.
     */
    @Override
    public void start() {
        LOG.info("Starting renewal task");
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping renewal task");
    }
}
