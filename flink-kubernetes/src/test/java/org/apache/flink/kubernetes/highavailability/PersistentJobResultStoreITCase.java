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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.EXECUTION_PLAN_STORE_KEY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for the persistent job result stores under the Kubernetes HA services of a {@code
 * MiniCluster}, against a real Kubernetes cluster. The test runs once per {@link
 * JobResultStoreOptions.StorageType}: the file system store and the ConfigMap store.
 */
class PersistentJobResultStoreITCase {

    private static final long TIMEOUT_MS = 60_000;
    private static final long RETRY_INTERVAL_MS = 100;
    private static final int RETRY_ATTEMPTS = (int) (TIMEOUT_MS / RETRY_INTERVAL_MS);

    @TempDir static Path temporaryPath;

    // Registered first, so that the assumption on the Kubernetes environment skips the test
    // before any cluster is started.
    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private final String clusterId = "jrs-itcase-" + UUID.randomUUID().toString().substring(0, 8);

    private final String clusterConfigMap =
            KubernetesLeaderElectionHaServices.getClusterConfigMap(clusterId);

    private final String jobResultStoreConfigMap =
            KubernetesLeaderElectionHaServices.getJobResultStoreConfigMap(clusterId);

    /**
     * A JobManager fails over after it wrote the dirty entry of a terminated job and before it
     * removed the job's execution plan. The new leader must not run the job again: it reads the
     * dirty entry, skips the plan, finishes the cleanup, and marks the entry as clean.
     *
     * <p>The failover is a leader change between the two Dispatcher components of a {@link
     * TestingMiniCluster}; each component has its own HA services, like two JobManager pods. The
     * test writes the dirty entry itself while the job runs, which leaves the same stored state as
     * a crash right after the Dispatcher's dirty write.
     */
    @ParameterizedTest
    @EnumSource(JobResultStoreOptions.StorageType.class)
    void testJobIsntResurrectedAfterFailover(JobResultStoreOptions.StorageType storageType)
            throws Exception {
        final Configuration configuration = createConfiguration(storageType);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(createBlockingVertex());
        final JobID jobId = jobGraph.getJobID();
        final JobResultStore jobResultStore = createJobResultStore(storageType, configuration);
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .setNumberDispatcherResourceManagerComponents(2)
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .build();
        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();
            final DispatcherGateway leader =
                    miniCluster.getDispatcherGatewayFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            miniCluster.submitJob(jobGraph).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            waitUntil(() -> miniCluster.getJobStatus(jobId).get() == JobStatus.RUNNING);

            jobResultStore
                    .createDirtyResultAsync(new JobResultEntry(createCanceledResult(jobId)))
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertThat(getStoredEntry(clusterConfigMap, planKey(jobId))).isPresent();

            leader.shutDownCluster();
            waitUntil(
                    () ->
                            !miniCluster
                                    .getDispatcherGatewayFuture()
                                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                    .getAddress()
                                    .equals(leader.getAddress()));

            // A resurrected job would run forever and keep the entry dirty.
            waitUntil(() -> jobResultStore.hasCleanJobResultEntryAsync(jobId).get());
            assertThat(jobResultStore.hasDirtyJobResultEntryAsync(jobId).get()).isFalse();
            assertThat(getStoredEntry(clusterConfigMap, planKey(jobId))).isEmpty();
            assertThat(miniCluster.getJobStatus(jobId).get(TIMEOUT_MS, TimeUnit.MILLISECONDS))
                    .isEqualTo(JobStatus.CANCELED);
        } finally {
            deleteConfigMaps();
        }
    }

    /** A store of the given type on the same storage as the cluster's own store. */
    private JobResultStore createJobResultStore(
            JobResultStoreOptions.StorageType storageType, Configuration configuration)
            throws IOException {
        switch (storageType) {
            case KUBERNETES:
                return new KubernetesJobResultStore(
                        kubernetesExtension.getFlinkKubeClient(),
                        jobResultStoreConfigMap,
                        clusterId,
                        () -> true,
                        configuration.get(JobResultStoreOptions.DELETE_ON_COMMIT),
                        Executors.directExecutor());
            case FILE_SYSTEM:
                return FileSystemJobResultStore.fromConfiguration(
                        configuration, Executors.directExecutor());
            default:
                throw new IllegalArgumentException("Unexpected storage type " + storageType);
        }
    }

    /** A job submitted on its own runs in a single-job application whose ID is the job ID. */
    private static JobResult createCanceledResult(JobID jobId) {
        return new JobResult.Builder()
                .jobId(jobId)
                .applicationId(ApplicationID.fromHexString(jobId.toHexString()))
                .jobStatus(JobStatus.CANCELED)
                .netRuntime(0)
                .build();
    }

    private static JobVertex createBlockingVertex() {
        final JobVertex vertex = new JobVertex("blocking");
        vertex.setInvokableClass(BlockingNoOpInvokable.class);
        vertex.setParallelism(1);
        return vertex;
    }

    private static String planKey(JobID jobId) {
        return EXECUTION_PLAN_STORE_KEY_PREFIX + jobId;
    }

    private static void waitUntil(SupplierWithException<Boolean, Exception> condition)
            throws Exception {
        CommonTestUtils.waitUntilCondition(condition, RETRY_INTERVAL_MS, RETRY_ATTEMPTS);
    }

    private static Optional<String> getStoredEntry(String configMapName, String key) {
        return kubernetesExtension
                .getFlinkKubeClient()
                .getConfigMap(configMapName)
                .map(configMap -> configMap.getData().get(key));
    }

    private void deleteConfigMaps() throws Exception {
        kubernetesExtension.getFlinkKubeClient().deleteConfigMap(jobResultStoreConfigMap).get();
        kubernetesExtension.getFlinkKubeClient().deleteConfigMap(clusterConfigMap).get();
    }

    private Configuration createConfiguration(JobResultStoreOptions.StorageType storageType) {
        final Configuration configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
        // On a deployed cluster the KubernetesClusterDescriptor copies the cluster ID into the HA
        // cluster ID; the file system store and the blob store derive their paths from the latter.
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        final String kubeConfigFile = System.getenv("ITCASE_KUBECONFIG");
        if (kubeConfigFile != null) {
            configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);
        }
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.KUBERNETES.name());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryPath.toAbsolutePath().toString());
        configuration.set(JobResultStoreOptions.STORAGE_TYPE, storageType);
        // Retain the clean entry so that the tests can inspect it.
        configuration.set(JobResultStoreOptions.DELETE_ON_COMMIT, false);
        return configuration;
    }
}
