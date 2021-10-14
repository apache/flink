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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.Optional;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Providing a mocked pulsar server. */
public class PulsarMockRuntime implements PulsarRuntime {

    private static final String CLUSTER_NAME = "mock-pulsar-" + randomAlphanumeric(6);
    private final MockPulsarService pulsarService;
    private PulsarRuntimeOperator operator;

    public PulsarMockRuntime() {
        this(createConfig());
    }

    public PulsarMockRuntime(ServiceConfiguration configuration) {
        this.pulsarService = new MockPulsarService(configuration);
    }

    @Override
    public void startUp() {
        try {
            pulsarService.start();
        } catch (PulsarServerException e) {
            throw new IllegalStateException(e);
        }
        this.operator =
                new PulsarRuntimeOperator(
                        pulsarService.getBrokerServiceUrl(), pulsarService.getWebServiceAddress());

        // Successfully start a pulsar broker, we have to create the required resources.
        sneakyAdmin(this::createTestResource);
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

    private void createTestResource() throws PulsarAdminException {
        PulsarAdmin admin = operator().admin();
        if (!admin.clusters().getClusters().contains(CLUSTER_NAME)) {
            // Make clients can test short names
            ClusterData data =
                    ClusterData.builder()
                            .serviceUrl("http://127.0.0.1:" + pulsarService.getBrokerServicePort())
                            .build();
            admin.clusters().createCluster(CLUSTER_NAME, data);
        }

        createOrUpdateTenant("public");
        createOrUpdateNamespace("public", "default");

        createOrUpdateTenant("pulsar");
        createOrUpdateNamespace("pulsar", "system");
    }

    private void createOrUpdateTenant(String tenant) throws PulsarAdminException {
        PulsarAdmin admin = operator().admin();
        TenantInfo info =
                TenantInfo.builder()
                        .adminRoles(ImmutableSet.of("appid1", "appid2"))
                        .allowedClusters(ImmutableSet.of(CLUSTER_NAME))
                        .build();
        if (!admin.tenants().getTenants().contains(tenant)) {
            admin.tenants().createTenant(tenant, info);
        } else {
            admin.tenants().updateTenant(tenant, info);
        }
    }

    public void createOrUpdateNamespace(String tenant, String namespace)
            throws PulsarAdminException {
        PulsarAdmin admin = operator().admin();
        String namespaceValue = tenant + "/" + namespace;
        if (!admin.namespaces().getNamespaces(tenant).contains(namespaceValue)) {
            admin.namespaces().createNamespace(namespaceValue);
            admin.namespaces().setRetention(namespaceValue, new RetentionPolicies(60, 1000));
        }
    }

    private static ServiceConfiguration createConfig() {
        ServiceConfiguration configuration = new ServiceConfiguration();

        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName(CLUSTER_NAME);

        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultRetentionTimeInMinutes(7);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setZookeeperServers("localhost:2181");
        configuration.setConfigurationStoreServers("localhost:3181");

        configuration.setAuthenticationEnabled(false);
        configuration.setAuthorizationEnabled(false);
        configuration.setAllowAutoTopicCreation(true);
        configuration.setBrokerDeleteInactiveTopicsEnabled(false);

        configuration.setWebSocketServiceEnabled(false);
        // Use runtime dynamic ports
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));

        // Enable transaction with in memory.
        configuration.setTransactionCoordinatorEnabled(true);
        configuration.setTransactionMetadataStoreProviderClassName(
                "org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider");
        configuration.setTransactionBufferProviderClassName(
                "org.apache.pulsar.broker.transaction.buffer.impl.InMemTransactionBufferProvider");

        return configuration;
    }
}
