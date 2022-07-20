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

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.LeaderInformationWithComponentId;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionListener;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link KubernetesMultipleComponentLeaderElectionDriver}. */
class KubernetesMultipleComponentLeaderElectionDriverTest {

    private static final String CLUSTER_ID = "test-cluster";
    private static final String LEADER_CONFIGMAP_NAME = "leader-configmap-name";
    private static final String LOCK_IDENTITY = "lock-identity";

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> testExecutorExtension =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testElectionDriverGainsLeadership() throws Exception {
        new TestFixture() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);
                        });
            }
        };
    }

    @Test
    void testElectionDriverLosesLeadership() throws Exception {
        new TestFixture() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);
                            getLeaderCallback().notLeader();
                            leaderElectionListener.await(LeaderElectionEvent.NotLeaderEvent.class);
                        });
            }
        };
    }

    @Test
    void testPublishLeaderInformation() throws Exception {
        new TestFixture() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class);
                            final LeaderInformation leaderInformation =
                                    LeaderInformation.known(UUID.randomUUID(), "localhost");
                            final String componentId = "componentId";

                            final DefaultLeaderRetrievalService leaderRetrievalService =
                                    new DefaultLeaderRetrievalService(
                                            new KubernetesMultipleComponentLeaderRetrievalDriverFactory(
                                                    getFlinkKubeClient(),
                                                    getConfigMapSharedWatcher(),
                                                    testExecutorExtension.getExecutor(),
                                                    LEADER_CONFIGMAP_NAME,
                                                    componentId));

                            final TestingListener leaderRetrievalListener = new TestingListener();
                            leaderRetrievalService.start(leaderRetrievalListener);

                            leaderElectionDriver.publishLeaderInformation(
                                    componentId, leaderInformation);

                            notifyLeaderRetrievalWatchOnModifiedConfigMap();

                            leaderRetrievalListener.waitForNewLeader();
                            assertThat(leaderRetrievalListener.getLeader())
                                    .isEqualTo(leaderInformation);
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangeNotifiesListener() throws Exception {
        new TestFixture() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            final String componentA = "componentA";
                            final LeaderInformation leaderInformationA =
                                    LeaderInformation.known(UUID.randomUUID(), "localhost");
                            final String componentB = "componentB";
                            final LeaderInformation leaderInformationB =
                                    LeaderInformation.known(UUID.randomUUID(), "localhost");
                            leaderElectionDriver.publishLeaderInformation(
                                    componentA, leaderInformationA);
                            leaderElectionDriver.publishLeaderInformation(
                                    componentB, leaderInformationB);

                            notifyLeaderElectionWatchOnModifiedConfigMap();

                            final LeaderElectionEvent.AllKnownLeaderInformationEvent
                                    allKnownLeaderInformationEvent =
                                            leaderElectionListener.await(
                                                    LeaderElectionEvent
                                                            .AllKnownLeaderInformationEvent.class);

                            assertThat(
                                            allKnownLeaderInformationEvent
                                                    .getLeaderInformationWithComponentIds())
                                    .containsExactlyInAnyOrder(
                                            LeaderInformationWithComponentId.create(
                                                    componentA, leaderInformationA),
                                            LeaderInformationWithComponentId.create(
                                                    componentB, leaderInformationB));
                        });
            }
        };
    }

    /** Test fixture for the {@link KubernetesMultipleComponentLeaderElectionDriverTest}. */
    protected class TestFixture {
        private final KubernetesTestFixture kubernetesTestFixture;
        final TestingLeaderElectionListener leaderElectionListener;
        final KubernetesMultipleComponentLeaderElectionDriver leaderElectionDriver;

        TestFixture() {
            kubernetesTestFixture =
                    new KubernetesTestFixture(CLUSTER_ID, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
            leaderElectionListener = new TestingLeaderElectionListener();
            leaderElectionDriver = createLeaderElectionDriver();
        }

        private KubernetesMultipleComponentLeaderElectionDriver createLeaderElectionDriver() {
            final KubernetesLeaderElectionConfiguration leaderElectionConfiguration =
                    new KubernetesLeaderElectionConfiguration(
                            LEADER_CONFIGMAP_NAME,
                            LOCK_IDENTITY,
                            kubernetesTestFixture.getConfiguration());
            return new KubernetesMultipleComponentLeaderElectionDriver(
                    leaderElectionConfiguration,
                    kubernetesTestFixture.getFlinkKubeClient(),
                    leaderElectionListener,
                    kubernetesTestFixture.getConfigMapSharedWatcher(),
                    testExecutorExtension.getExecutor(),
                    testingFatalErrorHandlerExtension.getTestingFatalErrorHandler());
        }

        void leaderCallbackGrantLeadership() throws Exception {
            kubernetesTestFixture.leaderCallbackGrantLeadership();
        }

        KubernetesLeaderElector.LeaderCallbackHandler getLeaderCallback() throws Exception {
            return kubernetesTestFixture.getLeaderCallback();
        }

        FlinkKubeClient getFlinkKubeClient() {
            return kubernetesTestFixture.getFlinkKubeClient();
        }

        KubernetesConfigMapSharedWatcher getConfigMapSharedWatcher() {
            return kubernetesTestFixture.getConfigMapSharedWatcher();
        }

        FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                getLeaderRetrievalConfigMapCallback() throws Exception {
            return kubernetesTestFixture.getLeaderRetrievalConfigMapCallback();
        }

        void notifyLeaderRetrievalWatchOnModifiedConfigMap() throws Exception {
            kubernetesTestFixture
                    .getLeaderRetrievalConfigMapCallback()
                    .onModified(
                            Collections.singletonList(kubernetesTestFixture.getLeaderConfigMap()));
        }

        void notifyLeaderElectionWatchOnModifiedConfigMap() throws Exception {
            kubernetesTestFixture
                    .getLeaderElectionConfigMapCallback()
                    .onModified(
                            Collections.singletonList(kubernetesTestFixture.getLeaderConfigMap()));
        }

        void runTest(RunnableWithException testMethod) throws Exception {
            try {
                testMethod.run();
            } finally {
                leaderElectionDriver.close();
                kubernetesTestFixture.close();
            }
        }
    }
}
