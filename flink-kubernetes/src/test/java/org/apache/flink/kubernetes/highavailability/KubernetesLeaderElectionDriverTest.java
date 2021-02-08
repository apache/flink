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

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.runtime.leaderelection.LeaderInformation;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link KubernetesLeaderElectionDriver}. */
public class KubernetesLeaderElectionDriverTest extends KubernetesHighAvailabilityTestBase {

    @Test
    public void testIsLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            // Grant leadership
                            leaderCallbackGrantLeadership();
                            assertThat(electionEventHandler.isLeader(), is(true));
                            assertThat(
                                    electionEventHandler.getConfirmedLeaderInformation(),
                                    is(LEADER_INFORMATION));
                        });
            }
        };
    }

    @Test
    public void testNotLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            // Revoke leadership
                            getLeaderCallback().notLeader();

                            electionEventHandler.waitForRevokeLeader(TIMEOUT);
                            assertThat(electionEventHandler.isLeader(), is(false));
                            assertThat(
                                    electionEventHandler.getConfirmedLeaderInformation(),
                                    is(LeaderInformation.empty()));
                            // The ConfigMap should also be cleared
                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_ADDRESS_KEY),
                                    is(nullValue()));
                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_SESSION_ID_KEY),
                                    is(nullValue()));
                        });
            }
        };
    }

    @Test
    public void testHasLeadershipWhenConfigMapNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionDriver.hasLeadership();
                            electionEventHandler.waitForError(TIMEOUT);
                            final String errorMsg =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist.";
                            assertThat(electionEventHandler.getError(), is(notNullValue()));
                            assertThat(
                                    electionEventHandler.getError(),
                                    FlinkMatchers.containsMessage(errorMsg));
                        });
            }
        };
    }

    @Test
    public void testWriteLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final LeaderInformation leader =
                                    LeaderInformation.known(UUID.randomUUID(), LEADER_URL);
                            leaderElectionDriver.writeLeaderInformation(leader);

                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_ADDRESS_KEY),
                                    is(leader.getLeaderAddress()));
                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_SESSION_ID_KEY),
                                    is(leader.getLeaderSessionID().toString()));
                        });
            }
        };
    }

    @Test
    public void testWriteLeaderInformationWhenConfigMapNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionDriver.writeLeaderInformation(LEADER_INFORMATION);
                            electionEventHandler.waitForError(TIMEOUT);

                            final String errorMsg =
                                    "Could not write leader information since ConfigMap "
                                            + LEADER_CONFIGMAP_NAME
                                            + " does not exist.";
                            assertThat(electionEventHandler.getError(), is(notNullValue()));
                            assertThat(
                                    electionEventHandler.getError(),
                                    FlinkMatchers.containsMessage(errorMsg));
                        });
            }
        };
    }

    @Test
    public void testLeaderConfigMapModifiedExternallyShouldBeCorrected() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            // Update ConfigMap with wrong data
                            final KubernetesConfigMap updatedConfigMap = getLeaderConfigMap();
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
                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_ADDRESS_KEY),
                                    is(LEADER_INFORMATION.getLeaderAddress()));
                            assertThat(
                                    getLeaderConfigMap().getData().get(LEADER_SESSION_ID_KEY),
                                    is(LEADER_INFORMATION.getLeaderSessionID().toString()));
                        });
            }
        };
    }

    @Test
    public void testLeaderConfigMapDeletedExternally() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.onDeleted(
                                    Collections.singletonList(getLeaderConfigMap()));

                            electionEventHandler.waitForError(TIMEOUT);
                            final String errorMsg =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " is deleted externally";
                            assertThat(electionEventHandler.getError(), is(notNullValue()));
                            assertThat(
                                    electionEventHandler.getError(),
                                    FlinkMatchers.containsMessage(errorMsg));
                        });
            }
        };
    }

    @Test
    public void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.onError(
                                    Collections.singletonList(getLeaderConfigMap()));

                            electionEventHandler.waitForError(TIMEOUT);
                            final String errorMsg =
                                    "Error while watching the ConfigMap " + LEADER_CONFIGMAP_NAME;
                            assertThat(electionEventHandler.getError(), is(notNullValue()));
                            assertThat(
                                    electionEventHandler.getError(),
                                    FlinkMatchers.containsMessage(errorMsg));
                        });
            }
        };
    }

    @Test
    public void testHighAvailabilityLabelsCorrectlySet() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final Map<String, String> leaderLabels =
                                    getLeaderConfigMap().getLabels();
                            assertThat(leaderLabels.size(), is(3));
                            assertThat(
                                    leaderLabels.get(LABEL_CONFIGMAP_TYPE_KEY),
                                    is(LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
                        });
            }
        };
    }

    @Test
    public void testNewWatchCreationWhenKubernetesTooOldResourceVersionException()
            throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderElectionConfigMapCallback();
                            callbackHandler.handleError(
                                    new KubernetesTooOldResourceVersionException(
                                            new Exception("too old resource version")));
                            // Verify the old watch is closed and a new one is created
                            assertThat(configMapWatches.size(), is(3));
                            // The three watches are [old-leader-election-watch,
                            // leader-retrieval-watch, new-leader-election-watch]
                            assertThat(
                                    configMapWatches.stream()
                                            .map(
                                                    TestingFlinkKubeClient.MockKubernetesWatch
                                                            ::isClosed)
                                            .collect(Collectors.toList()),
                                    contains(true, false, false));
                        });
            }
        };
    }
}
