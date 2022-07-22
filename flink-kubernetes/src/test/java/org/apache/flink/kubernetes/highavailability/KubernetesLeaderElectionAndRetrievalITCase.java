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
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionEventHandler;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.util.ExecutorUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT Tests for the {@link KubernetesLeaderElectionDriver} and {@link
 * KubernetesLeaderRetrievalDriver}. We expect the {@link KubernetesLeaderElectionDriver} could
 * become the leader and {@link KubernetesLeaderRetrievalDriver} could retrieve the leader address
 * from Kubernetes.
 */
class KubernetesLeaderElectionAndRetrievalITCase {

    private static final String LEADER_CONFIGMAP_NAME = "leader-test-cluster";
    private static final String LEADER_ADDRESS =
            "akka.tcp://flink@172.20.1.21:6123/user/rpc/dispatcher";

    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    @Test
    void testLeaderElectionAndRetrieval() throws Exception {
        final String configMapName = LEADER_CONFIGMAP_NAME + System.currentTimeMillis();
        KubernetesLeaderElectionDriver leaderElectionDriver = null;
        KubernetesLeaderRetrievalDriver leaderRetrievalDriver = null;

        final FlinkKubeClient flinkKubeClient = kubernetesExtension.getFlinkKubeClient();
        final Configuration configuration = kubernetesExtension.getConfiguration();

        final String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
        final KubernetesConfigMapSharedWatcher configMapSharedWatcher =
                flinkKubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
        final ExecutorService watchExecutorService = Executors.newCachedThreadPool();

        final TestingLeaderElectionEventHandler electionEventHandler =
                new TestingLeaderElectionEventHandler(LEADER_ADDRESS);

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
                            KubernetesUtils::getLeaderInformationFromConfigMap,
                            retrievalEventHandler::handleError);

            electionEventHandler.waitForLeader();
            // Check the new leader is confirmed
            final LeaderInformation confirmedLeaderInformation =
                    electionEventHandler.getConfirmedLeaderInformation();
            assertThat(confirmedLeaderInformation.getLeaderAddress()).isEqualTo(LEADER_ADDRESS);

            // Check the leader retrieval driver should be notified the leader address
            retrievalEventHandler.waitForNewLeader();
            assertThat(retrievalEventHandler.getLeaderSessionID())
                    .isEqualByComparingTo(confirmedLeaderInformation.getLeaderSessionID());
            assertThat(retrievalEventHandler.getAddress())
                    .isEqualTo(confirmedLeaderInformation.getLeaderAddress());
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
