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
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.TestingLeaderCallbackHandler;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_CONFIGMAP_NAME;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * IT Tests for the {@link KubernetesStateHandleStore}. We expect only the leader could update the
 * state store. The standby JobManagers update operations should not be issued. This is a
 * "check-leadership-and-update" behavior test. It is a very basic requirement for {@link
 * org.apache.flink.runtime.jobmanager.JobGraphStore} and {@link
 * org.apache.flink.runtime.checkpoint.CompletedCheckpointStore} implementation for Kubernetes.
 */
public class KubernetesStateHandleStoreITCase extends TestLogger {

    @ClassRule public static KubernetesResource kubernetesResource = new KubernetesResource();

    private final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();

    private static final long TIMEOUT = 120L * 1000L;

    private static final String KEY = "state-handle-test";

    @Test
    public void testMultipleKubernetesStateHandleStores() throws Exception {
        final Configuration configuration = kubernetesResource.getConfiguration();

        final String leaderConfigMapName = LEADER_CONFIGMAP_NAME + System.currentTimeMillis();
        final int leaderNum = 3;

        final KubernetesLeaderElector[] leaderElectors = new KubernetesLeaderElector[leaderNum];
        final FlinkKubeClient[] kubeClients = new FlinkKubeClient[leaderNum];
        final TestingLeaderCallbackHandler[] leaderCallbackHandlers =
                new TestingLeaderCallbackHandler[leaderNum];

        @SuppressWarnings("unchecked")
        final KubernetesStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>[]
                stateHandleStores = new KubernetesStateHandleStore[leaderNum];

        try {
            for (int i = 0; i < leaderNum; i++) {
                final String lockIdentity = UUID.randomUUID().toString();
                kubeClients[i] = kubeClientFactory.fromConfiguration(configuration, "testing");
                leaderCallbackHandlers[i] = new TestingLeaderCallbackHandler(lockIdentity);
                leaderElectors[i] =
                        kubeClients[i].createLeaderElector(
                                new KubernetesLeaderElectionConfiguration(
                                        leaderConfigMapName, lockIdentity, configuration),
                                leaderCallbackHandlers[i]);

                stateHandleStores[i] =
                        new KubernetesStateHandleStore<>(
                                kubeClients[i],
                                leaderConfigMapName,
                                new TestingLongStateHandleHelper(),
                                (ignore) -> true,
                                lockIdentity);

                leaderElectors[i].run();
            }

            // Wait for the leader
            final String lockIdentity =
                    TestingLeaderCallbackHandler.waitUntilNewLeaderAppears(TIMEOUT);
            Long expectedState = null;

            for (int i = 0; i < leaderNum; i++) {
                // leader
                if (leaderCallbackHandlers[i].getLockIdentity().equals(lockIdentity)) {
                    expectedState = (long) i;
                }
                stateHandleStores[i].addAndLock(
                        KEY, new TestingLongStateHandleHelper.LongStateHandle(i));
            }

            // Only the leader could add successfully
            assertThat(expectedState, is(notNullValue()));
            assertThat(stateHandleStores[0].getAllAndLock().size(), is(1));
            assertThat(
                    stateHandleStores[0].getAllAndLock().get(0).f0.retrieveState().getValue(),
                    is(expectedState));
            assertThat(stateHandleStores[0].getAllAndLock().get(0).f1, is(KEY));
        } finally {
            TestingLongStateHandleHelper.clearGlobalState();
            // Cleanup the resources
            for (int i = 0; i < leaderNum; i++) {
                if (leaderElectors[i] != null) {
                    leaderElectors[i].stop();
                }
                if (kubeClients[i] != null) {
                    kubeClients[i].close();
                }
            }
            kubernetesResource.getFlinkKubeClient().deleteConfigMap(leaderConfigMapName).get();
        }
    }
}
