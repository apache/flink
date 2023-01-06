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
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;

import static org.apache.flink.runtime.security.token.DefaultDelegationTokenManager.isProviderEnabled;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Repository for delegation token receivers. */
@Internal
public class DelegationTokenReceiverRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(DelegationTokenReceiverRepository.class);

    private final Configuration configuration;

    @Nullable private final PluginManager pluginManager;

    @VisibleForTesting final Map<String, DelegationTokenReceiver> delegationTokenReceivers;

    public DelegationTokenReceiverRepository(
            Configuration configuration, @Nullable PluginManager pluginManager) {
        this.configuration = checkNotNull(configuration, "Flink configuration must not be null");
        this.pluginManager = pluginManager;
        this.delegationTokenReceivers = loadReceivers();
    }

    private Map<String, DelegationTokenReceiver> loadReceivers() {
        LOG.info("Loading delegation token receivers");

        Map<String, DelegationTokenReceiver> receivers = new HashMap<>();
        Consumer<DelegationTokenReceiver> loadReceiver =
                (receiver) -> {
                    try {
                        if (isProviderEnabled(configuration, receiver.serviceName())) {
                            receiver.init(configuration);
                            LOG.info(
                                    "Delegation token receiver {} loaded and initialized",
                                    receiver.serviceName());
                            checkState(
                                    !receivers.containsKey(receiver.serviceName()),
                                    "Delegation token receiver with service name {} has multiple implementations",
                                    receiver.serviceName());
                            receivers.put(receiver.serviceName(), receiver);
                        } else {
                            LOG.info(
                                    "Delegation token receiver {} is disabled so not loaded",
                                    receiver.serviceName());
                        }
                    } catch (Exception | NoClassDefFoundError e) {
                        // The intentional general rule is that if a receiver's init method throws
                        // exception
                        // then stop the workload
                        LOG.error(
                                "Failed to initialize delegation token receiver {}",
                                receiver.serviceName(),
                                e);
                        throw new FlinkRuntimeException(e);
                    }
                };
        ServiceLoader.load(DelegationTokenReceiver.class).iterator().forEachRemaining(loadReceiver);
        if (pluginManager != null) {
            pluginManager.load(DelegationTokenReceiver.class).forEachRemaining(loadReceiver);
        }

        LOG.info("Delegation token receivers loaded successfully");

        return receivers;
    }

    @VisibleForTesting
    boolean isReceiverLoaded(String serviceName) {
        return delegationTokenReceivers.containsKey(serviceName);
    }

    /**
     * Callback function when new delegation tokens obtained.
     *
     * @param containerBytes Serialized form of a DelegationTokenContainer. All the available tokens
     *     will be forwarded to the appropriate {@link DelegationTokenReceiver} based on service
     *     name.
     */
    public void onNewTokensObtained(byte[] containerBytes) throws Exception {
        if (containerBytes == null || containerBytes.length == 0) {
            throw new IllegalArgumentException("Illegal container tried to be processed");
        }
        DelegationTokenContainer container =
                InstantiationUtil.deserializeObject(
                        containerBytes, DelegationTokenContainer.class.getClassLoader());
        onNewTokensObtained(container);
    }

    /**
     * Callback function when new delegation tokens obtained.
     *
     * @param container Serialized form of delegation tokens stored in DelegationTokenContainer. All
     *     the available tokens will be forwarded to the appropriate {@link DelegationTokenReceiver}
     *     based on service name.
     */
    public void onNewTokensObtained(DelegationTokenContainer container) throws Exception {
        LOG.info("New delegation tokens arrived, sending them to receivers");
        for (Map.Entry<String, byte[]> entry : container.getTokens().entrySet()) {
            String serviceName = entry.getKey();
            byte[] tokens = entry.getValue();
            if (!delegationTokenReceivers.containsKey(serviceName)) {
                throw new IllegalStateException(
                        "Tokens arrived for service but no receiver found for it: " + serviceName);
            }
            try {
                delegationTokenReceivers.get(serviceName).onNewTokensObtained(tokens);
            } catch (Exception e) {
                LOG.warn("Failed to send tokens to delegation token receiver {}", serviceName, e);
            }
        }
        LOG.info("Delegation tokens sent to receivers");
    }
}
