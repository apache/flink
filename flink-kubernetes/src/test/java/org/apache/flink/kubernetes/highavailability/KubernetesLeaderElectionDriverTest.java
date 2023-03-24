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
import org.apache.flink.runtime.leaderelection.LeaderInformation;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
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
                            // Grant leadership
                            leaderCallbackGrantLeadership();
                            assertThat(electionEventHandler.isLeader()).isTrue();
                            assertThat(
                                            electionEventHandler
                                                    .getConfirmedLeaderInformation()
                                                    .getLeaderAddress())
                                    .isEqualTo(LEADER_ADDRESS);
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

                            electionEventHandler.waitForRevokeLeader();
                            assertThat(electionEventHandler.isLeader()).isFalse();
                            assertThat(electionEventHandler.getConfirmedLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
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
                            electionEventHandler.waitForError();
                            final String errorMsg =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist.";
                            assertThat(electionEventHandler.getError()).isNotNull();
                            assertThatChainOfCauses(electionEventHandler.getError())
                                    .anySatisfy(t -> assertThat(t).hasMessageContaining(errorMsg));
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
                            leaderCallbackGrantLeadership();

                            final LeaderInformation leader =
                                    LeaderInformation.known(UUID.randomUUID(), LEADER_ADDRESS);
                            leaderElectionDriver.writeLeaderInformation(leader);

                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(LEADER_ADDRESS_KEY, leader.getLeaderAddress());
                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(
                                            LEADER_SESSION_ID_KEY,
                                            leader.getLeaderSessionID().toString());
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
                            leaderElectionDriver.writeLeaderInformation(
                                    LeaderInformation.known(UUID.randomUUID(), LEADER_ADDRESS));
                            electionEventHandler.waitForError();

                            final String errorMsg =
                                    "Could not write leader information since ConfigMap "
                                            + LEADER_CONFIGMAP_NAME
                                            + " does not exist.";
                            assertThat(electionEventHandler.getError()).isNotNull();
                            assertThatChainOfCauses(electionEventHandler.getError())
                                    .anySatisfy(t -> assertThat(t).hasMessageContaining(errorMsg));
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
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            // Update ConfigMap with wrong data
                            final KubernetesConfigMap updatedConfigMap = getLeaderConfigMap();
                            final UUID leaderSessionId =
                                    UUID.fromString(
                                            updatedConfigMap.getData().get(LEADER_SESSION_ID_KEY));
                            final LeaderInformation faultyLeader =
                                    LeaderInformation.known(
                                            UUID.randomUUID(), "faultyLeaderAddress");
                            updatedConfigMap
                                    .getData()
                                    .put(LEADER_ADDRESS_KEY, faultyLeader.getLeaderAddress());
                            updatedConfigMap
                                    .getData()
                                    .put(
                                            LEADER_SESSION_ID_KEY,
                                            faultyLeader.getLeaderSessionID().toString());

                            callbackHandler.onModified(Collections.singletonList(updatedConfigMap));
                            // The leader should be corrected
                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(LEADER_ADDRESS_KEY, LEADER_ADDRESS);
                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(
                                            LEADER_SESSION_ID_KEY, leaderSessionId.toString());
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

                            electionEventHandler.waitForError();
                            final String errorMsg =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " is deleted externally";
                            assertThat(electionEventHandler.getError()).isNotNull();
                            assertThatChainOfCauses(electionEventHandler.getError())
                                    .anySatisfy(t -> assertThat(t).hasMessageContaining(errorMsg));
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

                            electionEventHandler.waitForError();
                            final String errorMsg =
                                    "Error while watching the ConfigMap " + LEADER_CONFIGMAP_NAME;
                            assertThat(electionEventHandler.getError()).isNotNull();
                            assertThatChainOfCauses(electionEventHandler.getError())
                                    .anySatisfy(t -> assertThat(t).hasMessageContaining(errorMsg));
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
}
