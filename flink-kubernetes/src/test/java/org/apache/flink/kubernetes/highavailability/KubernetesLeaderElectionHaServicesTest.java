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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector.LEADER_ANNOTATION_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the job result store selection of {@link KubernetesLeaderElectionHaServices}. */
class KubernetesLeaderElectionHaServicesTest {

    private static final String CLUSTER_ID = "ha-services-test-cluster";

    @TempDir File temporaryFolder;

    private KubernetesTestFixture fixture;
    private FlinkKubeClient kubeClient;
    private Configuration configuration;

    @BeforeEach
    void setup() {
        fixture =
                new KubernetesTestFixture(
                        CLUSTER_ID,
                        CLUSTER_ID + "-cluster-config-map",
                        UUID.randomUUID().toString());
        kubeClient = fixture.getFlinkKubeClient();
        configuration = fixture.getConfiguration();
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toURI().toString());
    }

    @AfterEach
    void teardown() {
        fixture.close();
    }

    @Test
    void testFileSystemJobResultStoreByDefault() throws Exception {
        final KubernetesLeaderElectionHaServices haServices = createHaServices();
        try {
            assertThat(haServices.getJobResultStore()).isInstanceOf(FileSystemJobResultStore.class);
        } finally {
            haServices.close();
        }
    }

    @Test
    void testKubernetesJobResultStoreWhenConfigured() throws Exception {
        configuration.set(
                JobResultStoreOptions.STORAGE_TYPE, JobResultStoreOptions.StorageType.KUBERNETES);

        final KubernetesLeaderElectionHaServices haServices = createHaServices();
        try {
            assertThat(haServices.getJobResultStore()).isInstanceOf(KubernetesJobResultStore.class);
        } finally {
            haServices.close();
        }
    }

    @Test
    void testCleanupDeletesJobResultStoreConfigMap() throws Exception {
        configuration.set(JobResultStoreOptions.DELETE_ON_COMMIT, true);

        assertThat(cleanupWithJobResultStoreConfigMap()).isEmpty();
    }

    @Test
    void testCleanupKeepsJobResultStoreConfigMapWithRetainedEntries() throws Exception {
        configuration.set(JobResultStoreOptions.DELETE_ON_COMMIT, false);

        assertThat(cleanupWithJobResultStoreConfigMap()).isPresent();
    }

    /** Cleans up Kubernetes HA services whose job result store ConfigMap exists. */
    private Optional<KubernetesConfigMap> cleanupWithJobResultStoreConfigMap() throws Exception {
        configuration.set(
                JobResultStoreOptions.STORAGE_TYPE, JobResultStoreOptions.StorageType.KUBERNETES);
        final String configMapName =
                KubernetesLeaderElectionHaServices.getJobResultStoreConfigMap(CLUSTER_ID);
        kubeClient
                .createConfigMap(new TestingFlinkKubeClient.MockKubernetesConfigMap(configMapName))
                .join();
        final KubernetesLeaderElectionHaServices haServices = createHaServices();

        haServices.closeWithOptionalClean(true);

        return kubeClient.getConfigMap(configMapName);
    }

    @Test
    void testLeadershipCheckReadsLeaderAnnotationOfClusterConfigMap() {
        final String clusterConfigMapName = CLUSTER_ID + "-cluster-config-map";
        final String lockIdentity = "this-job-manager";
        final BooleanSupplier leadershipCheck =
                KubernetesLeaderElectionHaServices.createLeadershipCheck(
                        kubeClient, clusterConfigMapName, lockIdentity);

        assertThat(leadershipCheck.getAsBoolean()).as("no cluster ConfigMap").isFalse();

        final TestingFlinkKubeClient.MockKubernetesConfigMap clusterConfigMap =
                new TestingFlinkKubeClient.MockKubernetesConfigMap(clusterConfigMapName);
        kubeClient.createConfigMap(clusterConfigMap).join();
        clusterConfigMap.getAnnotations().put(LEADER_ANNOTATION_KEY, "another-job-manager");
        assertThat(leadershipCheck.getAsBoolean()).as("another leader").isFalse();

        clusterConfigMap.getAnnotations().put(LEADER_ANNOTATION_KEY, lockIdentity);
        assertThat(leadershipCheck.getAsBoolean()).as("this leader").isTrue();
    }

    private KubernetesLeaderElectionHaServices createHaServices() throws Exception {
        return new KubernetesLeaderElectionHaServices(
                kubeClient, Executors.directExecutor(), configuration, new VoidBlobStore());
    }
}
