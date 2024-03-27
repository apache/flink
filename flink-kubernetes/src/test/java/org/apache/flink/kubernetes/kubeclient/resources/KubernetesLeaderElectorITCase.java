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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT Tests for the {@link KubernetesLeaderElector}. Start multiple leader contenders currently, one
 * should elect successfully. And if current leader dies, a new one could take over.
 */
class KubernetesLeaderElectorITCase {
    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();

    private String configMapName;

    @BeforeEach
    void initializeConfigMapName() {
        this.configMapName =
                String.format(
                        "%s-configmap-%s",
                        // needs to comply to RFC-1123
                        KubernetesLeaderElectorITCase.class.getSimpleName().toLowerCase(),
                        UUID.randomUUID());
    }

    @AfterEach
    void deleteConfigMapName() {
        kubernetesExtension.getFlinkKubeClient().deleteConfigMap(this.configMapName).join();
    }

    @Test
    void testMultipleKubernetesLeaderElectors() throws Exception {
        final Configuration configuration = kubernetesExtension.getConfiguration();

        final int leaderNum = 3;

        final KubernetesLeaderElector[] leaderElectors = new KubernetesLeaderElector[leaderNum];
        // We use different Kubernetes clients for different leader electors.
        final FlinkKubeClient[] kubeClients = new FlinkKubeClient[leaderNum];
        final TestingLeaderCallbackHandler[] leaderCallbackHandlers =
                new TestingLeaderCallbackHandler[leaderNum];

        try {
            for (int i = 0; i < leaderNum; i++) {
                kubeClients[i] = kubeClientFactory.fromConfiguration(configuration, "testing");
                leaderCallbackHandlers[i] =
                        new TestingLeaderCallbackHandler(UUID.randomUUID().toString());
                final KubernetesLeaderElectionConfiguration leaderConfig =
                        new KubernetesLeaderElectionConfiguration(
                                configMapName,
                                leaderCallbackHandlers[i].getLockIdentity(),
                                configuration);
                leaderElectors[i] =
                        kubeClients[i].createLeaderElector(leaderConfig, leaderCallbackHandlers[i]);

                // Start the leader electors to contend the leader
                leaderElectors[i].run();
            }

            // Wait for the first leader
            final String firstLockIdentity =
                    TestingLeaderCallbackHandler.waitUntilNewLeaderAppears();

            for (int i = 0; i < leaderNum; i++) {
                if (leaderCallbackHandlers[i].getLockIdentity().equals(firstLockIdentity)) {
                    // Check the callback isLeader is called.
                    leaderCallbackHandlers[i].waitForNewLeader();
                    assertThat(leaderCallbackHandlers[i].hasLeadership()).isTrue();
                    // Current leader died
                    leaderElectors[i].stop();
                    // Check the callback notLeader is called.
                    leaderCallbackHandlers[i].waitForRevokeLeader();
                    assertThat(leaderCallbackHandlers[i].hasLeadership()).isFalse();
                } else {
                    assertThat(leaderCallbackHandlers[i].hasLeadership()).isFalse();
                }
            }

            // Another leader should be elected successfully and update the lock identity
            final String anotherLockIdentity =
                    TestingLeaderCallbackHandler.waitUntilNewLeaderAppears();
            assertThat(anotherLockIdentity).isNotEqualTo(firstLockIdentity);
        } finally {
            // Cleanup the resources
            for (int i = 0; i < leaderNum; i++) {
                if (leaderElectors[i] != null) {
                    leaderElectors[i].stop();
                }
                if (kubeClients[i] != null) {
                    kubeClients[i].close();
                }
            }
        }
    }

    @Test
    void testClusterConfigMapLabelsAreSet() throws Exception {
        final Configuration configuration = kubernetesExtension.getConfiguration();

        final TestingLeaderCallbackHandler leaderCallbackHandler =
                new TestingLeaderCallbackHandler(UUID.randomUUID().toString());
        final KubernetesLeaderElectionConfiguration leaderConfig =
                new KubernetesLeaderElectionConfiguration(
                        configMapName, leaderCallbackHandler.getLockIdentity(), configuration);

        try (FlinkKubeClient kubeClient =
                kubeClientFactory.fromConfiguration(configuration, "testing")) {
            final KubernetesLeaderElector leaderElector =
                    kubeClient.createLeaderElector(leaderConfig, leaderCallbackHandler);
            try {
                leaderElector.run();

                TestingLeaderCallbackHandler.waitUntilNewLeaderAppears();

                assertThat(kubeClient.getConfigMap(configMapName))
                        .hasValueSatisfying(
                                configMap ->
                                        assertThat(configMap.getLabels())
                                                .hasSize(3)
                                                .containsEntry(
                                                        LABEL_CONFIGMAP_TYPE_KEY,
                                                        LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
            } finally {
                leaderElector.stop();
            }
        }
    }

    /**
     * This test verifies that the {@link KubernetesLeaderElector} is able to handle scenario where
     * the lease cannot be renewed.
     *
     * <p>See FLINK-34007 for further details.
     */
    @Test
    void testLeaderElectorLifecycleManagement() throws Exception {
        final Configuration configuration = kubernetesExtension.getConfiguration();

        try (final NamespacedKubernetesClient client =
                kubeClientFactory.createFabric8ioKubernetesClient(configuration)) {

            // set a low timeout that makes the client stop renewing the leadership lease
            final Duration renewTimeout = Duration.ofMillis(100);
            configuration.set(
                    KubernetesHighAvailabilityOptions.KUBERNETES_RENEW_DEADLINE, renewTimeout);

            final String lockIdentity = UUID.randomUUID().toString();
            final KubernetesLeaderElectionConfiguration leaderConfig =
                    new KubernetesLeaderElectionConfiguration(
                            configMapName, lockIdentity, configuration);
            final TestingLeaderCallbackHandler leadershipCallbackHandler =
                    new TestingLeaderCallbackHandler(lockIdentity);

            final ManuallyTriggeredScheduledExecutorService executorService =
                    new ManuallyTriggeredScheduledExecutorService();
            final KubernetesLeaderElector testInstance =
                    new KubernetesLeaderElector(
                            client, leaderConfig, leadershipCallbackHandler, executorService);

            // first leadership lifecycle initiation
            testInstance.run();

            // triggers acquiring the leadership
            final Duration waitForNextTaskForever = Duration.ofDays(10);
            executorService.trigger(waitForNextTaskForever);

            assertThatFuture(leadershipCallbackHandler.waitForNewLeaderAsync())
                    .as("The leadership should be acquired eventually.")
                    .eventuallySucceeds();

            // halt thread to reach the renew deadline
            Thread.sleep(renewTimeout.plusSeconds(1).toMillis());

            // triggers renew loop within fabric8io's LeaderElector
            executorService.trigger();

            assertThatFuture(leadershipCallbackHandler.waitForRevokeLeaderAsync())
                    .as(
                            "The leadership should be lost eventually due to the renewal loop being stopped.")
                    .eventuallySucceeds();

            // revoking the leadership initiates another leadership lifecycle
            testInstance.run();
            executorService.trigger(waitForNextTaskForever);

            assertThatFuture(leadershipCallbackHandler.waitForNewLeaderAsync())
                    .as("The leadership should be acquired eventually again.");
        }
    }

    @Test
    void testKubernetesLeaderElectorSendingLeadershipLostSignalOnStop() {
        final Configuration configuration = kubernetesExtension.getConfiguration();

        try (final NamespacedKubernetesClient client =
                kubeClientFactory.createFabric8ioKubernetesClient(configuration)) {

            final String lockIdentity = UUID.randomUUID().toString();
            final KubernetesLeaderElectionConfiguration leaderConfig =
                    new KubernetesLeaderElectionConfiguration(
                            configMapName, lockIdentity, configuration);
            final TestingLeaderCallbackHandler leadershipCallbackHandler =
                    new TestingLeaderCallbackHandler(lockIdentity);

            final ManuallyTriggeredScheduledExecutorService executorService =
                    new ManuallyTriggeredScheduledExecutorService();
            final KubernetesLeaderElector testInstance =
                    new KubernetesLeaderElector(
                            client, leaderConfig, leadershipCallbackHandler, executorService);

            // initiate leadership lifecycle
            testInstance.run();

            final Duration waitForNextTaskForever = Duration.ofDays(10);
            executorService.trigger(waitForNextTaskForever);
            assertThatFuture(leadershipCallbackHandler.waitForNewLeaderAsync())
                    .as("Leadership should be acquired eventually.")
                    .eventuallySucceeds();

            testInstance.stop();

            assertThat(leadershipCallbackHandler.hasLeadership())
                    .as("Leadership should be lost right away after stopping the test instance.")
                    .isFalse();

            assertThatFuture(leadershipCallbackHandler.waitForRevokeLeaderAsync())
                    .as("There should be a leadership lost event being received eventually.")
                    .eventuallySucceeds();
        }
    }
}
