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
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.TestingLeaderCallbackHandler;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.testutils.TestingApplicationResultStore;
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
 * IT case for {@link KubernetesApplicationResultStore} against a real Kubernetes cluster: several
 * JobManagers share one application result store ConfigMap, and only the leader may write to it.
 */
class KubernetesApplicationResultStoreITCase {

    private static final String CLUSTER_ID_PREFIX = "application-result-store-itcase-cluster-";
    private static final int JOB_MANAGER_NUM = 3;

    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();

    @Test
    void testLeaderFencing() throws Exception {
        final Configuration configuration = kubernetesExtension.getConfiguration();
        final String clusterId = CLUSTER_ID_PREFIX + System.currentTimeMillis();
        final String clusterConfigMapName = clusterId + "-cluster-config-map";
        final String configMapName = clusterId + "-application-result-store-config-map";

        final FlinkKubeClient[] kubeClients = new FlinkKubeClient[JOB_MANAGER_NUM];
        final KubernetesLeaderElector[] leaderElectors =
                new KubernetesLeaderElector[JOB_MANAGER_NUM];
        final TestingLeaderCallbackHandler[] leaderCallbackHandlers =
                new TestingLeaderCallbackHandler[JOB_MANAGER_NUM];
        final KubernetesApplicationResultStore[] stores =
                new KubernetesApplicationResultStore[JOB_MANAGER_NUM];
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
                stores[i] =
                        new KubernetesApplicationResultStore(
                                kubeClients[i],
                                configMapName,
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

            final ApplicationID applicationId = new ApplicationID();
            final ApplicationResult result =
                    TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);
            final ApplicationResultEntry entry = new ApplicationResultEntry(result);
            final String key = applicationId.toHexString();
            final String storedResult =
                    ApplicationState.FINISHED.name()
                            + ":"
                            + result.getStartTime()
                            + ":"
                            + result.getEndTime()
                            + ":"
                            + result.getApplicationName();
            final String dirtyValue = KubernetesApplicationResultStore.DIRTY_PREFIX + storedResult;
            final String cleanValue = KubernetesApplicationResultStore.CLEAN_PREFIX + storedResult;

            // The non-leaders are fenced; they must write first, because the thread-safe base
            // class rejects a second dirty entry for the same application before the fencing
            // applies.
            for (int i = 0; i < JOB_MANAGER_NUM; i++) {
                if (i != leader) {
                    final KubernetesApplicationResultStore nonLeaderStore = stores[i];
                    assertThatThrownBy(() -> nonLeaderStore.createDirtyResultAsync(entry).join())
                            .hasCauseInstanceOf(IOException.class)
                            .hasMessageContaining("leadership");
                }
            }
            assertThat(getData(configMapName)).doesNotContainKey(key);

            stores[leader].createDirtyResultAsync(entry).join();
            assertThat(getData(configMapName)).containsOnly(entry(key, dirtyValue));

            // Reads are not fenced, and the entry round-trips through the API server.
            final KubernetesApplicationResultStore nonLeaderStore =
                    stores[(leader + 1) % JOB_MANAGER_NUM];
            final ApplicationResult recovered = nonLeaderStore.getDirtyResults().iterator().next();
            assertThat(recovered.getApplicationId()).isEqualTo(applicationId);
            assertThat(recovered.getApplicationState()).isEqualTo(ApplicationState.FINISHED);
            assertThat(recovered.getApplicationName()).isEqualTo(result.getApplicationName());

            assertThatThrownBy(() -> nonLeaderStore.markResultAsCleanAsync(applicationId).join())
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("leadership");
            assertThat(getData(configMapName)).containsOnly(entry(key, dirtyValue));

            stores[leader].markResultAsCleanAsync(applicationId).join();
            assertThat(getData(configMapName)).containsOnly(entry(key, cleanValue));
            assertThat(nonLeaderStore.hasCleanApplicationResultEntryAsync(applicationId).join())
                    .isTrue();
            assertThat(nonLeaderStore.getCleanApplicationResultAsync(applicationId).join())
                    .satisfies(
                            clean -> {
                                assertThat(clean.getApplicationId()).isEqualTo(applicationId);
                                assertThat(clean.getApplicationState())
                                        .isEqualTo(ApplicationState.FINISHED);
                            });
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
            kubernetesExtension.getFlinkKubeClient().deleteConfigMap(configMapName).get();
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
