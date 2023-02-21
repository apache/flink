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
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionEventHandler;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.TestingLeaderRetrievalEventHandler;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;
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
            "akka.tcp://flink@172.20.1.21:6123/user/rpc/dispatcher";
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

        final LeaderElectionDriver leaderElectionDriver;
        final TestingLeaderElectionEventHandler electionEventHandler;

        final LeaderRetrievalDriver leaderRetrievalDriver;
        final TestingLeaderRetrievalEventHandler retrievalEventHandler;

        final FlinkKubeClient flinkKubeClient;
        final Configuration configuration;

        final CompletableFuture<Void> closeKubeClientFuture;
        final CompletableFuture<Map<String, String>> deleteConfigMapByLabelsFuture;

        Context() {
            kubernetesTestFixture =
                    new KubernetesTestFixture(CLUSTER_ID, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
            flinkKubeClient = kubernetesTestFixture.getFlinkKubeClient();
            configuration = kubernetesTestFixture.getConfiguration();
            closeKubeClientFuture = kubernetesTestFixture.getCloseKubeClientFuture();
            deleteConfigMapByLabelsFuture =
                    kubernetesTestFixture.getDeleteConfigMapByLabelsFuture();
            electionEventHandler = new TestingLeaderElectionEventHandler(LEADER_ADDRESS);
            leaderElectionDriver = createLeaderElectionDriver();

            retrievalEventHandler = new TestingLeaderRetrievalEventHandler();
            leaderRetrievalDriver = createLeaderRetrievalDriver();
        }

        void runTest(RunnableWithException testMethod) throws Exception {
            electionEventHandler.init(leaderElectionDriver);

            try {
                testMethod.run();
            } finally {
                electionEventHandler.close();
                leaderElectionDriver.close();
                leaderRetrievalDriver.close();
                kubernetesTestFixture.close();
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

        // Use the leader callback to manually grant leadership
        void leaderCallbackGrantLeadership() throws Exception {
            kubernetesTestFixture.leaderCallbackGrantLeadership();
            electionEventHandler.waitForLeader();
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

        private LeaderElectionDriver createLeaderElectionDriver() {
            final KubernetesLeaderElectionConfiguration leaderConfig =
                    new KubernetesLeaderElectionConfiguration(
                            LEADER_CONFIGMAP_NAME, LOCK_IDENTITY, configuration);
            final KubernetesLeaderElectionDriverFactory factory =
                    new KubernetesLeaderElectionDriverFactory(
                            flinkKubeClient,
                            kubernetesTestFixture.getConfigMapSharedWatcher(),
                            watchCallbackExecutorService,
                            leaderConfig);
            return factory.createLeaderElectionDriver(
                    electionEventHandler, electionEventHandler::handleError, LEADER_ADDRESS);
        }

        private LeaderRetrievalDriver createLeaderRetrievalDriver() {
            final KubernetesLeaderRetrievalDriverFactory factory =
                    new KubernetesLeaderRetrievalDriverFactory(
                            flinkKubeClient,
                            kubernetesTestFixture.getConfigMapSharedWatcher(),
                            watchCallbackExecutorService,
                            LEADER_CONFIGMAP_NAME);
            return factory.createLeaderRetrievalDriver(
                    retrievalEventHandler, retrievalEventHandler::handleError);
        }
    }
}
