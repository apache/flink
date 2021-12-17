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
import org.apache.flink.kubernetes.KubernetesResource;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionEventHandler;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_CONFIGMAP_NAME;
import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_INFORMATION;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * IT Tests for the {@link KubernetesLeaderElectionDriver} and {@link
 * KubernetesLeaderRetrievalDriver}. We expect the {@link KubernetesLeaderElectionDriver} could
 * become the leader and {@link KubernetesLeaderRetrievalDriver} could retrieve the leader address
 * from Kubernetes.
 */
public class KubernetesLeaderElectionAndRetrievalITCase extends TestLogger {

    @ClassRule public static KubernetesResource kubernetesResource = new KubernetesResource();

    private static final long TIMEOUT = 120L * 1000L;

    @Test
    public void testLeaderElectionAndRetrieval() throws Exception {
        final String configMapName = LEADER_CONFIGMAP_NAME + System.currentTimeMillis();
        KubernetesLeaderElectionDriver leaderElectionDriver = null;
        KubernetesLeaderRetrievalDriver leaderRetrievalDriver = null;

        final FlinkKubeClient flinkKubeClient = kubernetesResource.getFlinkKubeClient();
        final Configuration configuration = kubernetesResource.getConfiguration();

        final String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
        final KubernetesConfigMapSharedWatcher configMapSharedWatcher =
                flinkKubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
        final ExecutorService watchExecutorService = Executors.newCachedThreadPool();

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_INFORMATION);

        try {
            leaderElectionDriver =
                    new KubernetesLeaderElectionDriver(
                            flinkKubeClient,
                            configMapSharedWatcher,
                            watchExecutorService,
                            new KubernetesLeaderElectionConfiguration(
                                    configMapName, UUID.randomUUID().toString(), configuration),
                            electionEventHandler,
                            electionEventHandler::handleError);
            electionEventHandler.init(leaderElectionDriver);

            final TestingLeaderRetrievalEventHandler retrievalEventHandler =
                    new TestingLeaderRetrievalEventHandler();
            leaderRetrievalDriver =
                    new KubernetesLeaderRetrievalDriver(
                            flinkKubeClient,
                            configMapSharedWatcher,
                            watchExecutorService,
                            configMapName,
                            retrievalEventHandler,
                            retrievalEventHandler::handleError);

            electionEventHandler.waitForLeader(TIMEOUT);
            // Check the new leader is confirmed
            assertThat(
                    electionEventHandler.getConfirmedLeaderInformation(), is(LEADER_INFORMATION));

            // Check the leader retrieval driver should be notified the leader address
            retrievalEventHandler.waitForNewLeader(TIMEOUT);
            assertThat(
                    retrievalEventHandler.getLeaderSessionID(),
                    is(LEADER_INFORMATION.getLeaderSessionID()));
            assertThat(
                    retrievalEventHandler.getAddress(), is(LEADER_INFORMATION.getLeaderAddress()));
        } finally {
            electionEventHandler.close();
            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
            }
            if (leaderRetrievalDriver != null) {
                leaderRetrievalDriver.close();
            }
            flinkKubeClient.deleteConfigMap(configMapName).get();
            configMapSharedWatcher.close();
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, watchExecutorService);
        }
    }
}
