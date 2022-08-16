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

package org.apache.flink.connector.pulsar.testutils.runtime;

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

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_ASSIGN;

/** This class is used to create the basic topics for a standalone Pulsar instance. */
public final class PulsarRuntimeUtils {

    private PulsarRuntimeUtils() {
        // No public constructor
    }

    /** Create the system topics. */
    public static void initializePulsarEnvironment(
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
}
