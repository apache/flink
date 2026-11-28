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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.TestingLeaderCallbackHandler;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * IT case for {@link KubernetesJobResultStore} against a real Kubernetes cluster: several
 * JobManagers share one job result store ConfigMap, and only the leader may write to it.
 */
class KubernetesJobResultStoreITCase {

    private static final String CLUSTER_ID_PREFIX = "job-result-store-itcase-cluster-";
    private static final int JOB_MANAGER_NUM = 3;

    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();

    @Test
    void testLeaderFencing() throws Exception {
        final Configuration configuration = kubernetesExtension.getConfiguration();
        final String clusterId = CLUSTER_ID_PREFIX + System.currentTimeMillis();
        final String clusterConfigMapName = clusterId + "-cluster-config-map";
        final String jobResultStoreConfigMapName =
                KubernetesLeaderElectionHaServices.getJobResultStoreConfigMap(clusterId);

        final FlinkKubeClient[] kubeClients = new FlinkKubeClient[JOB_MANAGER_NUM];
        final KubernetesLeaderElector[] leaderElectors =
                new KubernetesLeaderElector[JOB_MANAGER_NUM];
        final TestingLeaderCallbackHandler[] leaderCallbackHandlers =
                new TestingLeaderCallbackHandler[JOB_MANAGER_NUM];
        final KubernetesJobResultStore[] jobResultStores =
                new KubernetesJobResultStore[JOB_MANAGER_NUM];
        try {
            for (int i = 0; i < JOB_MANAGER_NUM; i++) {
                final String lockIdentity = UUID.randomUUID().toString();
                kubeClients[i] = kubeClientFactory.fromConfiguration(configuration, "testing");
                leaderCallbackHandlers[i] = new TestingLeaderCallbackHandler(lockIdentity);
                leaderElectors[i] =
                        kubeClients[i].createLeaderElector(
                                new KubernetesLeaderElectionConfiguration(
                                        clusterConfigMapName, lockIdentity, configuration),
                                leaderCallbackHandlers[i]);
                jobResultStores[i] =
                        new KubernetesJobResultStore(
                                kubeClients[i],
                                jobResultStoreConfigMapName,
                                clusterId,
                                KubernetesLeaderElectionHaServices.createLeadershipCheck(
                                        kubeClients[i], clusterConfigMapName, lockIdentity),
                                false,
                                Executors.directExecutor());
                leaderElectors[i].run();
            }
            final String leaderLockIdentity =
                    TestingLeaderCallbackHandler.waitUntilNewLeaderAppears();
            int leader = -1;
            for (int i = 0; i < JOB_MANAGER_NUM; i++) {
                if (leaderCallbackHandlers[i].getLockIdentity().equals(leaderLockIdentity)) {
                    leader = i;
                }
            }
            assertThat(leader).isNotNegative();

            final JobID jobId = new JobID();
            final JobResultEntry entry =
                    new JobResultEntry(TestingJobResultStore.createSuccessfulJobResult(jobId));
            final String key = jobId.toHexString();
            final String dirtyValue =
                    JobStatus.FINISHED.name()
                            + ":"
                            + entry.getJobResult().getApplicationId().get().toHexString();

            // The non-leaders are fenced; they must write first, because the thread-safe base
            // class rejects a second dirty entry for the same job before the fencing applies.
            for (int i = 0; i < JOB_MANAGER_NUM; i++) {
                if (i != leader) {
                    final KubernetesJobResultStore nonLeaderStore = jobResultStores[i];
                    assertThatThrownBy(() -> nonLeaderStore.createDirtyResultAsync(entry).join())
                            .hasCauseInstanceOf(IOException.class)
                            .hasMessageContaining("leadership");
                }
            }
            assertThat(getData(jobResultStoreConfigMapName)).doesNotContainKey(key);

            jobResultStores[leader].createDirtyResultAsync(entry).join();
            assertThat(getData(jobResultStoreConfigMapName)).containsOnly(entry(key, dirtyValue));

            // Reads are not fenced, and the entry round-trips through the API server.
            final JobResult recovered =
                    jobResultStores[(leader + 1) % JOB_MANAGER_NUM]
                            .getDirtyResults()
                            .iterator()
                            .next();
            assertThat(recovered.getJobId()).isEqualTo(jobId);
            assertThat(recovered.getJobStatus()).hasValue(JobStatus.FINISHED);
            assertThat(recovered.getApplicationId())
                    .isEqualTo(entry.getJobResult().getApplicationId());

            final KubernetesJobResultStore nonLeaderStore =
                    jobResultStores[(leader + 1) % JOB_MANAGER_NUM];
            assertThatThrownBy(() -> nonLeaderStore.markResultAsCleanAsync(jobId).join())
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("leadership");
            assertThat(getData(jobResultStoreConfigMapName)).containsOnly(entry(key, dirtyValue));

            jobResultStores[leader].markResultAsCleanAsync(jobId).join();
            assertThat(getData(jobResultStoreConfigMapName))
                    .containsOnly(entry(key, KubernetesJobResultStore.CLEAN_VALUE));
            assertThat(nonLeaderStore.hasCleanJobResultEntryAsync(jobId).join()).isTrue();
        } finally {
            for (int i = 0; i < JOB_MANAGER_NUM; i++) {
                if (leaderElectors[i] != null) {
                    leaderElectors[i].stop();
                }
                if (kubeClients[i] != null) {
                    kubeClients[i].close();
                }
            }
            kubernetesExtension.getFlinkKubeClient().deleteConfigMap(clusterConfigMapName).get();
            kubernetesExtension
                    .getFlinkKubeClient()
                    .deleteConfigMap(jobResultStoreConfigMapName)
                    .get();
        }
    }

    private static Map<String, String> getData(String configMapName) {
        return kubernetesExtension
                .getFlinkKubeClient()
                .getConfigMap(configMapName)
                .map(KubernetesConfigMap::getData)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "ConfigMap " + configMapName + " does not exist."));
    }
}
