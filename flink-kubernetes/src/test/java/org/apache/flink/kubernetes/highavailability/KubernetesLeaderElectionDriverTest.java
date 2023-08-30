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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.LeaderElectionException;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.LeaderInformationRegister;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link KubernetesLeaderElectionDriver}. */
class KubernetesLeaderElectionDriverTest extends KubernetesHighAvailabilityTestBase {

    @Test
    void testIsLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            grantLeadership();

                            // the following call shouldn't block
                            electionEventHandler.await(LeaderElectionEvent.IsLeaderEvent.class);
                        });
            }
        };
    }

    @Test
    void testNotLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            // Revoke leadership
                            getLeaderCallback().notLeader();

                            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);

                            // The ConfigMap should also be cleared
                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(LEADER_ADDRESS_KEY);
                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(LEADER_SESSION_ID_KEY);
                        });
            }
        };
    }

    @Test
    void testHasLeadershipWhenConfigMapNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionDriver.hasLeadership();

                            final String expectedErrorMessage =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist.";

                            final LeaderElectionEvent.ErrorEvent errorEvent =
                                    electionEventHandler.await(
                                            LeaderElectionEvent.ErrorEvent.class);
                            assertThat(errorEvent.getError())
                                    .isInstanceOf(KubernetesException.class)
                                    .hasMessageContaining(expectedErrorMessage);
                        });
            }
        };
    }

    @Test
    void testWriteLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final UUID leaderSessionID = leaderCallbackGrantLeadership();

                            final LeaderInformation leaderInformation =
                                    LeaderInformation.known(leaderSessionID, leaderAddress);

                            assertThat(getLeaderInformationFromConfigMap())
                                    .hasValue(leaderInformation);
                        });
            }
        };
    }

    @Test
    void testWriteLeaderInformationWhenConfigMapNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionDriver.publishLeaderInformation(
                                    componentId,
                                    LeaderInformation.known(UUID.randomUUID(), LEADER_ADDRESS));

                            final String expectedErrorMessage =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist.";
                            final LeaderElectionEvent.ErrorEvent errorEvent =
                                    electionEventHandler.assertNextEvent(
                                            LeaderElectionEvent.ErrorEvent.class);
                            assertThat(errorEvent.getError())
                                    .cause()
                                    .isInstanceOf(KubernetesException.class)
                                    .hasMessage(expectedErrorMessage);
                        });
            }
        };
    }

    @Test
    void testLeaderConfigMapModifiedExternallyShouldBeCorrected() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final UUID leaderSessionID = leaderCallbackGrantLeadership();

                            final Optional<LeaderInformation> leaderInformation =
                                    getLeaderInformationFromConfigMap();
                            assertThat(leaderInformation.map(LeaderInformation::getLeaderSessionID))
                                    .hasValue(leaderSessionID);

                            // Update ConfigMap with wrong data
                            final UUID faultySessionID = UUID.randomUUID();
                            final String faultyAddress = "faultyLeaderAddress";
                            putLeaderInformationIntoConfigMap(faultySessionID, faultyAddress);

                            // Simulate ConfigMap modification event
                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.onModified(
                                    Collections.singletonList(getLeaderConfigMap()));

                            final LeaderInformationRegister leaderInformationRegister =
                                    electionEventHandler
                                            .await(
                                                    LeaderElectionEvent
                                                            .AllLeaderInformationChangeEvent.class)
                                            .getLeaderInformationRegister();

                            assertThat(leaderInformationRegister.getRegisteredComponentIds())
                                    .containsExactly(componentId);

                            final LeaderInformation expectedFaultyLeaderInformation =
                                    LeaderInformation.known(faultySessionID, faultyAddress);
                            assertThat(leaderInformationRegister.forComponentId(componentId))
                                    .hasValue(expectedFaultyLeaderInformation);

                            leaderElectionDriver.publishLeaderInformation(
                                    componentId,
                                    LeaderInformation.known(leaderSessionID, leaderAddress));

                            assertThat(getLeaderInformationFromConfigMap())
                                    .as("LeaderInformation should have been corrected.")
                                    .hasValue(
                                            LeaderInformation.known(
                                                    leaderSessionID, leaderAddress));
                        });
            }
        };
    }

    @Test
    void testLeaderConfigMapDeletedExternally() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.onDeleted(
                                    Collections.singletonList(getLeaderConfigMap()));

                            final String expectedErrorMessage =
                                    "ConfigMap "
                                            + LEADER_CONFIGMAP_NAME
                                            + " has been deleted externally.";
                            final LeaderElectionEvent.ErrorEvent errorEvent =
                                    electionEventHandler.await(
                                            LeaderElectionEvent.ErrorEvent.class);
                            assertThat(errorEvent.getError())
                                    .isInstanceOf(LeaderElectionException.class)
                                    .hasMessage(expectedErrorMessage);
                        });
            }
        };
    }

    @Test
    void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.onError(
                                    Collections.singletonList(getLeaderConfigMap()));

                            final String expectedErrorMessage =
                                    "Error while watching the ConfigMap "
                                            + LEADER_CONFIGMAP_NAME
                                            + ".";

                            final LeaderElectionEvent.ErrorEvent errorEvent =
                                    electionEventHandler.await(
                                            LeaderElectionEvent.ErrorEvent.class);

                            assertThat(errorEvent.getError())
                                    .isInstanceOf(LeaderElectionException.class)
                                    .hasMessage(expectedErrorMessage);
                        });
            }
        };
    }

    @Test
    void testHighAvailabilityLabelsCorrectlySet() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final Map<String, String> leaderLabels =
                                    getLeaderConfigMap().getLabels();
                            assertThat(leaderLabels).hasSize(3);
                            assertThat(leaderLabels)
                                    .containsEntry(
                                            LABEL_CONFIGMAP_TYPE_KEY,
                                            LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
                        });
            }
        };
    }

    @Test
    void testToStringContainingConfigMap() throws Exception {
        new Context() {
            {
                runTest(
                        () ->
                                assertThat(leaderElectionDriver.toString())
                                        .as(
                                                "toString() should contain the ConfigMap name for a human-readable representation of the driver instance.")
                                        .contains(LEADER_CONFIGMAP_NAME));
            }
        };
    }
}
