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
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Base class for high availability unit tests with a configured testing Kubernetes client. */
class KubernetesHighAvailabilityTestBase {
    private static final String CLUSTER_ID = "leader-test-cluster";

    public static final String LOCK_IDENTITY = UUID.randomUUID().toString();
    public static final String LEADER_ADDRESS =
            "pekko.tcp://flink@172.20.1.21:6123/user/rpc/dispatcher";
    public static final String LEADER_CONFIGMAP_NAME = "leader-test-cluster";

    protected static final long TIMEOUT = 30L * 1000L;

    protected ExecutorService executorService;
    protected ExecutorService watchCallbackExecutorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newFixedThreadPool(4, new ExecutorThreadFactory("IO-Executor"));
        watchCallbackExecutorService =
                Executors.newCachedThreadPool(new ExecutorThreadFactory("Watch-Callback"));
    }

    @AfterEach
    void teardown() {
        ExecutorUtils.gracefulShutdown(
                TIMEOUT, TimeUnit.MILLISECONDS, watchCallbackExecutorService, executorService);
    }

    /** Context to leader election and retrieval tests. */
    protected class Context {
        private final KubernetesTestFixture kubernetesTestFixture;

        final String componentId;
        final String leaderAddress;
        final LeaderElectionDriver leaderElectionDriver;
        final TestingLeaderElectionListener electionEventHandler;

        final LeaderRetrievalDriver leaderRetrievalDriver;
        final TestingLeaderRetrievalEventHandler retrievalEventHandler;

        final FlinkKubeClient flinkKubeClient;
        final Configuration configuration;

        final CompletableFuture<Void> closeKubeClientFuture;
        final CompletableFuture<Map<String, String>> deleteConfigMapByLabelsFuture;

        Context() throws Exception {
            kubernetesTestFixture =
                    new KubernetesTestFixture(CLUSTER_ID, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);

            final UUID randomTestID = UUID.randomUUID();
            componentId = "random-component-id-" + randomTestID;
            leaderAddress = "random-address-" + randomTestID;

            flinkKubeClient = kubernetesTestFixture.getFlinkKubeClient();
            configuration = kubernetesTestFixture.getConfiguration();
            closeKubeClientFuture = kubernetesTestFixture.getCloseKubeClientFuture();
            deleteConfigMapByLabelsFuture =
                    kubernetesTestFixture.getDeleteConfigMapByLabelsFuture();
            electionEventHandler = new TestingLeaderElectionListener();
            leaderElectionDriver = createLeaderElectionDriver();

            retrievalEventHandler = new TestingLeaderRetrievalEventHandler();
            leaderRetrievalDriver = createLeaderRetrievalDriver();
        }

        void runTest(RunnableWithException testMethod) throws Exception {
            try {
                testMethod.run();
            } finally {
                leaderElectionDriver.close();
                leaderRetrievalDriver.close();
                kubernetesTestFixture.close();

                electionEventHandler.failIfErrorEventHappened();
            }
        }

        TestingFlinkKubeClient.Builder createFlinkKubeClientBuilder() {
            return kubernetesTestFixture.createFlinkKubeClientBuilder();
        }

        String getClusterId() {
            return CLUSTER_ID;
        }

        KubernetesConfigMap getLeaderConfigMap() {
            return kubernetesTestFixture.getLeaderConfigMap();
        }

        String getLeaderInformationKey() {
            return KubernetesUtils.createSingleLeaderKey(componentId);
        }

        Optional<LeaderInformation> getLeaderInformationFromConfigMap() {
            return KubernetesUtils.parseLeaderInformationSafely(
                    getLeaderConfigMap().getData().get(getLeaderInformationKey()));
        }

        void putLeaderInformationIntoConfigMap(UUID leaderSessionID, String leaderAddress) {
            getLeaderConfigMap()
                    .getData()
                    .put(
                            getLeaderInformationKey(),
                            KubernetesUtils.encodeLeaderInformation(
                                    LeaderInformation.known(leaderSessionID, leaderAddress)));
        }

        void grantLeadership() throws Exception {
            kubernetesTestFixture.leaderCallbackGrantLeadership();
        }

        // Use the leader callback to manually grant leadership
        UUID leaderCallbackGrantLeadership() throws Exception {
            grantLeadership();
            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);

            final UUID leaderSessionID = UUID.randomUUID();
            leaderElectionDriver.publishLeaderInformation(
                    componentId, LeaderInformation.known(leaderSessionID, leaderAddress));

            return leaderSessionID;
        }

        FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                getLeaderElectionConfigMapCallback() throws Exception {
            return kubernetesTestFixture.getLeaderElectionConfigMapCallback();
        }

        FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                getLeaderRetrievalConfigMapCallback() throws Exception {
            return kubernetesTestFixture.getLeaderRetrievalConfigMapCallback();
        }

        KubernetesLeaderElector.LeaderCallbackHandler getLeaderCallback() throws Exception {
            return kubernetesTestFixture.getLeaderCallback();
        }

        private LeaderElectionDriver createLeaderElectionDriver() throws Exception {
            final KubernetesLeaderElectionConfiguration leaderConfig =
                    new KubernetesLeaderElectionConfiguration(
                            LEADER_CONFIGMAP_NAME, LOCK_IDENTITY, configuration);
            final KubernetesLeaderElectionDriverFactory factory =
                    new KubernetesLeaderElectionDriverFactory(
                            flinkKubeClient,
                            leaderConfig,
                            kubernetesTestFixture.getConfigMapSharedWatcher(),
                            watchCallbackExecutorService);
            return factory.create(electionEventHandler);
        }

        private LeaderRetrievalDriver createLeaderRetrievalDriver() {
            final KubernetesLeaderRetrievalDriverFactory factory =
                    new KubernetesLeaderRetrievalDriverFactory(
                            kubernetesTestFixture.getConfigMapSharedWatcher(),
                            watchCallbackExecutorService,
                            LEADER_CONFIGMAP_NAME,
                            componentId);
            return factory.createLeaderRetrievalDriver(
                    retrievalEventHandler, retrievalEventHandler::handleError);
        }
    }
}
