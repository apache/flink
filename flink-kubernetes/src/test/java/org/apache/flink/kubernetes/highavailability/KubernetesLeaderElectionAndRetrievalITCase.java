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
import org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionListener;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            "pekko.tcp://flink@172.20.1.21:6123/user/rpc/dispatcher";

    @RegisterExtension
    static final KubernetesExtension KUBERNETES_EXTENSION = new KubernetesExtension();

    @RegisterExtension
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(Executors::newCachedThreadPool);

    @Test
    void testLeaderElectionAndRetrieval() throws Exception {
        final String componentId = "component-id";
        final String leaderAddress = "random-address";
        final String configMapName = LEADER_CONFIGMAP_NAME + UUID.randomUUID();
        final FlinkKubeClient flinkKubeClient = KUBERNETES_EXTENSION.getFlinkKubeClient();
        final Configuration configuration = KUBERNETES_EXTENSION.getConfiguration();

        final String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);

        // This will make the leader election retrieval time out if we won't process already
        // existing leader information when starting it up.
        configuration.set(
                KubernetesHighAvailabilityOptions.KUBERNETES_LEASE_DURATION, Duration.ofHours(1));
        configuration.set(
                KubernetesHighAvailabilityOptions.KUBERNETES_RETRY_PERIOD, Duration.ofHours(1));
        configuration.set(
                KubernetesHighAvailabilityOptions.KUBERNETES_RENEW_DEADLINE, Duration.ofHours(1));

        final List<AutoCloseable> closeables = new ArrayList<>();

        final KubernetesConfigMapSharedWatcher configMapSharedWatcher =
                flinkKubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
        closeables.add(configMapSharedWatcher);

        final TestingLeaderElectionListener electionEventHandler =
                new TestingLeaderElectionListener();
        try {
            final KubernetesLeaderElectionDriver leaderElectionDriver =
                    new KubernetesLeaderElectionDriver(
                            new KubernetesLeaderElectionConfiguration(
                                    configMapName, UUID.randomUUID().toString(), configuration),
                            flinkKubeClient,
                            electionEventHandler,
                            configMapSharedWatcher,
                            EXECUTOR_EXTENSION.getExecutor());
            closeables.add(leaderElectionDriver);

            final KubernetesLeaderRetrievalDriverFactory driverFactory =
                    new KubernetesLeaderRetrievalDriverFactory(
                            configMapSharedWatcher,
                            EXECUTOR_EXTENSION.getExecutor(),
                            configMapName,
                            componentId);

            final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
            final TestingLeaderRetrievalEventHandler firstLeaderRetrievalEventHandler =
                    new TestingLeaderRetrievalEventHandler();
            closeables.add(
                    driverFactory.createLeaderRetrievalDriver(
                            firstLeaderRetrievalEventHandler, fatalErrorHandler));

            // Wait for the driver to obtain leadership.
            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);
            final LeaderInformation leaderInformation =
                    LeaderInformation.known(UUID.randomUUID(), leaderAddress);
            leaderElectionDriver.publishLeaderInformation(componentId, leaderInformation);

            // Check if the leader retrieval driver is notified about the leader address
            awaitLeadership(firstLeaderRetrievalEventHandler, leaderInformation);

            // Start a second leader retrieval that should be notified immediately because we
            // already know who the leader is.
            final TestingLeaderRetrievalEventHandler secondRetrievalEventHandler =
                    new TestingLeaderRetrievalEventHandler();
            closeables.add(
                    driverFactory.createLeaderRetrievalDriver(
                            secondRetrievalEventHandler, fatalErrorHandler));
            awaitLeadership(secondRetrievalEventHandler, leaderInformation);
        } finally {
            for (AutoCloseable closeable : closeables) {
                closeable.close();
            }

            flinkKubeClient.deleteConfigMap(configMapName).get();

            electionEventHandler.failIfErrorEventHappened();
        }
    }

    private static void awaitLeadership(
            TestingLeaderRetrievalEventHandler handler, LeaderInformation leaderInformation)
            throws Exception {
        handler.waitForNewLeader();
        assertThat(handler.getLeaderSessionID()).isEqualTo(leaderInformation.getLeaderSessionID());
        assertThat(handler.getAddress()).isEqualTo(leaderInformation.getLeaderAddress());
    }
}
