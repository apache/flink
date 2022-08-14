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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_ASSIGN;

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

    /** Create the system topics. */
    private static void initializePulsarEnvironment(
            ServiceConfiguration config, String serviceUrl, String adminUrl)
            throws PulsarAdminException, PulsarClientException {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            ClusterData clusterData =
                    ClusterData.builder().serviceUrl(adminUrl).brokerServiceUrl(serviceUrl).build();
            String cluster = config.getClusterName();
            createSampleNameSpace(admin, clusterData, cluster);

            // Create default namespace
            createNameSpace(
                    admin,
                    cluster,
                    TopicName.PUBLIC_TENANT,
                    TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE);

            // Create Pulsar system namespace
            createNameSpace(
                    admin, cluster, SYSTEM_NAMESPACE.getTenant(), SYSTEM_NAMESPACE.toString());
            // Enable transaction
            if (config.isTransactionCoordinatorEnabled()
                    && !admin.namespaces()
                            .getTopics(SYSTEM_NAMESPACE.toString())
                            .contains(TRANSACTION_COORDINATOR_ASSIGN.getPartition(0).toString())) {
                admin.topics().createPartitionedTopic(TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
            }
        }
    }

    private static void createSampleNameSpace(
            PulsarAdmin admin, ClusterData clusterData, String cluster)
            throws PulsarAdminException {
        // Create a sample namespace
        String tenant = "sample";
        String globalCluster = "global";
        String namespace = tenant + "/ns1";

        List<String> clusters = admin.clusters().getClusters();
        if (!clusters.contains(cluster)) {
            admin.clusters().createCluster(cluster, clusterData);
        } else {
            admin.clusters().updateCluster(cluster, clusterData);
        }
        // Create marker for "global" cluster
        if (!clusters.contains(globalCluster)) {
            admin.clusters().createCluster(globalCluster, ClusterData.builder().build());
        }

        if (!admin.tenants().getTenants().contains(tenant)) {
            admin.tenants()
                    .createTenant(
                            tenant,
                            new TenantInfoImpl(
                                    Collections.emptySet(), Collections.singleton(cluster)));
        }

        if (!admin.namespaces().getNamespaces(tenant).contains(namespace)) {
            admin.namespaces().createNamespace(namespace);
        }
    }

    private static void createNameSpace(
            PulsarAdmin admin, String cluster, String publicTenant, String defaultNamespace)
            throws PulsarAdminException {
        if (!admin.tenants().getTenants().contains(publicTenant)) {
            admin.tenants()
                    .createTenant(
                            publicTenant,
                            TenantInfo.builder()
                                    .adminRoles(Collections.emptySet())
                                    .allowedClusters(Collections.singleton(cluster))
                                    .build());
        }
        if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
            admin.namespaces().createNamespace(defaultNamespace);
            admin.namespaces()
                    .setNamespaceReplicationClusters(
                            defaultNamespace, Collections.singleton(cluster));
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

        // Enable transactions.
        configuration.setSystemTopicEnabled(true);
        configuration.setBrokerDeduplicationEnabled(true);
        configuration.setTransactionCoordinatorEnabled(true);
        configuration.setTransactionMetadataStoreProviderClassName(
                "org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStoreProvider");

        return configuration;
    }
}
