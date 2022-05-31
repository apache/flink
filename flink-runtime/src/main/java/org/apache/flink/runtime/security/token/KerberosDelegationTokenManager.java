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
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_RELOGIN_PERIOD;
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
public class KerberosDelegationTokenManager implements DelegationTokenManager {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosDelegationTokenManager.class);

    private final Configuration configuration;

    private final KerberosRenewalPossibleProvider kerberosRenewalPossibleProvider;

    @VisibleForTesting final Map<String, DelegationTokenProvider> delegationTokenProviders;

    @Nullable private final ScheduledExecutor scheduledExecutor;

    @Nullable private final ExecutorService ioExecutor;

    @Nullable private ScheduledFuture<?> tgtRenewalFuture;

    public KerberosDelegationTokenManager(
            Configuration configuration,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.scheduledExecutor = scheduledExecutor;
        this.ioExecutor = ioExecutor;
        this.kerberosRenewalPossibleProvider =
                new KerberosRenewalPossibleProvider(new SecurityConfiguration(configuration));
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
    public void start() throws Exception {
        checkNotNull(scheduledExecutor, "Scheduled executor must not be null");
        checkNotNull(ioExecutor, "IO executor must not be null");
        checkState(tgtRenewalFuture == null, "Manager is already started");

        if (!kerberosRenewalPossibleProvider.isRenewalPossible()) {
            LOG.info("Renewal is NOT possible, skipping to start renewal task");
            return;
        }

        startTGTRenewal();
    }

    void startTGTRenewal() throws IOException {
        LOG.debug("Starting TGT renewal task");

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (currentUser.isFromKeytab()) {
            // In Hadoop 2.x, renewal of the keytab-based login seems to be automatic, but in Hadoop
            // 3.x, it is configurable (see hadoop.kerberos.keytab.login.autorenewal.enabled, added
            // in HADOOP-9567). This task will make sure that the user stays logged in regardless of
            // that configuration's value. Note that checkTGTAndReloginFromKeytab() is a no-op if
            // the TGT does not need to be renewed yet.
            long tgtRenewalPeriod = configuration.get(KERBEROS_RELOGIN_PERIOD).toMillis();
            tgtRenewalFuture =
                    scheduledExecutor.scheduleAtFixedRate(
                            () ->
                                    ioExecutor.execute(
                                            () -> {
                                                try {
                                                    LOG.debug("Renewing TGT");
                                                    currentUser.checkTGTAndReloginFromKeytab();
                                                    LOG.debug("TGT renewed successfully");
                                                } catch (Exception e) {
                                                    LOG.warn("Error while renewing TGT", e);
                                                }
                                            }),
                            tgtRenewalPeriod,
                            tgtRenewalPeriod,
                            TimeUnit.MILLISECONDS);
            LOG.debug("TGT renewal task started and reoccur in {} ms", tgtRenewalPeriod);
        } else {
            LOG.debug("TGT renewal task not started");
        }
    }

    void stopTGTRenewal() {
        if (tgtRenewalFuture != null) {
            tgtRenewalFuture.cancel(true);
            tgtRenewalFuture = null;
        }
    }

    /** Stops re-occurring token obtain task. */
    @Override
    public void stop() {
        LOG.info("Stopping credential renewal");

        stopTGTRenewal();

        LOG.info("Stopped credential renewal");
    }
}
