/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionListener;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

@ExtendWith(TestLoggerExtension.class)
public class KubernetesMultipleComponentLeaderElectionDriverTest {

    private static final String CLUSTER_ID = "test-cluster";

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @RegisterExtension
    private final TestExecutorExtension<ExecutorService> testExecutorExtension =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    @Test
    public void testElectionDriverGainsLeadership() throws InterruptedException {
        final Configuration configuration = new Configuration();
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        final KubernetesLeaderElectionConfiguration leaderElectionConfiguration =
                new KubernetesLeaderElectionConfiguration("foobar", "barfoo", configuration);

        CompletableFuture<KubernetesLeaderElector.LeaderCallbackHandler>
                leaderCallbackHandlerFuture = new CompletableFuture<>();
        final FlinkKubeClient flinkKubeClient =
                TestingFlinkKubeClient.builder()
                        .setCreateLeaderElectorFunction(
                                (leaderConfig, callbackHandler) -> {
                                    leaderCallbackHandlerFuture.complete(callbackHandler);
                                    return new TestingFlinkKubeClient
                                            .TestingKubernetesLeaderElector(
                                            leaderConfig, callbackHandler);
                                })
                        .build();

        final KubernetesConfigMapSharedWatcher configMapSharedWatcher =
                flinkKubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                CLUSTER_ID, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));

        final TestingLeaderElectionListener leaderElectionListener =
                new TestingLeaderElectionListener();

        final KubernetesMultipleComponentLeaderElectionDriver leaderElectionDriver =
                new KubernetesMultipleComponentLeaderElectionDriver(
                        leaderElectionConfiguration,
                        flinkKubeClient,
                        leaderElectionListener,
                        configMapSharedWatcher,
                        testExecutorExtension.getExecutor(),
                        testingFatalErrorHandlerExtension.getTestingFatalErrorHandler());

        final KubernetesLeaderElector.LeaderCallbackHandler leaderCallbackHandler =
                leaderCallbackHandlerFuture.join();

        leaderCallbackHandler.isLeader();

        leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);
    }

    @Test
    public void testElectionDriverLosesLeadership() throws Exception {}

    @Test
    public void testPublishLeaderInformation() throws Exception {}

    @Test
    public void testLeaderInformationChange() throws Exception {}

    @Test
    public void testLeaderElectionWithMultipleDrivers() throws Exception {}
}
