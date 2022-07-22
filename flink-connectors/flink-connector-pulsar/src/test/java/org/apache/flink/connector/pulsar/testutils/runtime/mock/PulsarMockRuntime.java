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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;

import org.apache.pulsar.broker.ServiceConfiguration;

import java.util.Optional;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeUtils.initializePulsarEnvironment;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Providing a mocked pulsar server. */
public class PulsarMockRuntime implements PulsarRuntime {

    private static final String CLUSTER_NAME = "mock-pulsar-" + randomAlphanumeric(6);
    private final ServiceConfiguration configuration;
    private final MockPulsarService pulsarService;
    private PulsarRuntimeOperator operator;

    public PulsarMockRuntime() {
        this(createConfig());
    }

    public PulsarMockRuntime(ServiceConfiguration configuration) {
        this.configuration = configuration;
        this.pulsarService = new MockPulsarService(configuration);
    }

    @Override
    public void startUp() {
        try {
            pulsarService.start();

            String serviceUrl = pulsarService.getBrokerServiceUrl();
            String adminUrl = pulsarService.getWebServiceAddress();
            initializePulsarEnvironment(configuration, serviceUrl, adminUrl);

            this.operator = new PulsarRuntimeOperator(serviceUrl, adminUrl);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void tearDown() {
        try {
            pulsarService.close();
            operator.close();
            this.operator = null;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this mock pulsar first.");
    }

    private static ServiceConfiguration createConfig() {
        ServiceConfiguration configuration = new ServiceConfiguration();

        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName(CLUSTER_NAME);

        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultRetentionTimeInMinutes(7);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setMetadataStoreUrl("memory:local");
        configuration.setConfigurationMetadataStoreUrl("memory:local");

        configuration.setAuthenticationEnabled(false);
        configuration.setAuthorizationEnabled(false);
        configuration.setAllowAutoTopicCreation(true);
        configuration.setBrokerDeleteInactiveTopicsEnabled(false);

        configuration.setWebSocketServiceEnabled(false);
        // Use runtime dynamic ports
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));

        // Enable transactions.
        configuration.setSystemTopicEnabled(true);
        configuration.setBrokerDeduplicationEnabled(true);
        configuration.setTransactionCoordinatorEnabled(true);
        configuration.setTransactionMetadataStoreProviderClassName(
                "org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStoreProvider");

        return configuration;
    }
}
