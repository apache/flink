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

import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.highavailability.ApplicationResultStoreOptions;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * IT case for the {@link KubernetesJobResultStore} and the {@link KubernetesApplicationResultStore}
 * wired through the Kubernetes HA services of a {@code MiniCluster} against a real Kubernetes
 * cluster: a terminated job ends up in the job result store ConfigMap, its application in the
 * application result store ConfigMap, and the Dispatcher rejects the job ID afterwards.
 */
class KubernetesJobResultStoreMiniClusterITCase {

    private static final long TIMEOUT = 60 * 1000;
    private static final String CLUSTER_ID =
            "job-result-store-minicluster-itcase-" + System.currentTimeMillis();

    @TempDir static Path temporaryPath;

    // The extension's beforeAll runs before the @BeforeAll method below, so the assumption on the
    // Kubernetes environment skips the class before any cluster is started.
    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private static MiniCluster miniCluster;

    @BeforeAll
    static void startMiniCluster() throws Exception {
        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(getConfiguration())
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(1)
                                .build());
        miniCluster.start();
    }

    @AfterAll
    static void stopMiniCluster() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    void testTerminatedJobIsRecordedInTheConfigMap() throws Exception {
        final String configMapName =
                KubernetesLeaderElectionHaServices.getJobResultStoreConfigMap(CLUSTER_ID);
        final String applicationConfigMapName =
                KubernetesLeaderElectionHaServices.getApplicationResultStoreConfigMap(CLUSTER_ID);
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        final JobID jobId = jobGraph.getJobID();
        final String key = jobId.toHexString();
        try {
            miniCluster.submitJob(jobGraph).get(TIMEOUT, TimeUnit.MILLISECONDS);
            assertThat(miniCluster.requestJobResult(jobId).get(TIMEOUT, TimeUnit.MILLISECONDS))
                    .satisfies(jobResult -> assertThat(jobResult.isSuccess()).isTrue());

            CommonTestUtils.waitUntilCondition(
                    () ->
                            getStoredEntry(configMapName, key)
                                    .filter(KubernetesJobResultStore.CLEAN_VALUE::equals)
                                    .isPresent());

            final KubernetesConfigMap configMap =
                    kubernetesExtension.getFlinkKubeClient().getConfigMap(configMapName).get();
            assertThat(configMap.getData())
                    .containsOnly(entry(key, KubernetesJobResultStore.CLEAN_VALUE));

            // The application of a job submitted on its own has the job's ID; it is marked as
            // clean after the job, and the clean entry keeps the result.
            final String cleanFinishedPrefix =
                    KubernetesApplicationResultStore.CLEAN_PREFIX
                            + ApplicationState.FINISHED.name()
                            + ":";
            CommonTestUtils.waitUntilCondition(
                    () ->
                            getStoredEntry(applicationConfigMapName, key)
                                    .filter(value -> value.startsWith(cleanFinishedPrefix))
                                    .isPresent());
            assertThat(
                            kubernetesExtension
                                    .getFlinkKubeClient()
                                    .getConfigMap(applicationConfigMapName)
                                    .get()
                                    .getData())
                    .containsOnlyKeys(key);

            final JobGraph duplicate = JobGraphTestUtils.singleNoOpJobGraph();
            duplicate.setJobID(jobId);
            assertThatThrownBy(
                            () ->
                                    miniCluster
                                            .submitJob(duplicate)
                                            .get(TIMEOUT, TimeUnit.MILLISECONDS))
                    .satisfies(
                            throwable ->
                                    assertThat(
                                                    ExceptionUtils.findThrowableWithMessage(
                                                            throwable, "already been submitted"))
                                            .isPresent());
        } finally {
            kubernetesExtension.getFlinkKubeClient().deleteConfigMap(configMapName).get();
            kubernetesExtension
                    .getFlinkKubeClient()
                    .deleteConfigMap(applicationConfigMapName)
                    .get();
        }
    }

    private static Optional<String> getStoredEntry(String configMapName, String key) {
        return kubernetesExtension
                .getFlinkKubeClient()
                .getConfigMap(configMapName)
                .map(configMap -> configMap.getData().get(key));
    }

    private static Configuration getConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        final String kubeConfigFile = System.getenv("ITCASE_KUBECONFIG");
        if (kubeConfigFile != null) {
            configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);
        }
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.KUBERNETES.name());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryPath.toAbsolutePath().toString());
        configuration.set(
                JobResultStoreOptions.STORAGE_TYPE, JobResultStoreOptions.StorageType.KUBERNETES);
        configuration.set(
                ApplicationResultStoreOptions.STORAGE_TYPE,
                ApplicationResultStoreOptions.StorageType.KUBERNETES);
        // Retain the clean entries so that the test can inspect them.
        configuration.set(JobResultStoreOptions.DELETE_ON_COMMIT, false);
        configuration.set(ApplicationResultStoreOptions.DELETE_ON_COMMIT, false);
        return configuration;
    }
}
