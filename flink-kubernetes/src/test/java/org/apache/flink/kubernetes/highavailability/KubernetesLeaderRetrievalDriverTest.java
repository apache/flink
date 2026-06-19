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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link KubernetesLeaderRetrievalDriver}. */
class KubernetesLeaderRetrievalDriverTest extends KubernetesHighAvailabilityTestBase {

    @Test
    void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderRetrievalConfigMapCallback();
                            callbackHandler.onError(
                                    Collections.singletonList(getLeaderConfigMap()));
                            final String errMsg =
                                    "Error while watching the ConfigMap " + LEADER_CONFIGMAP_NAME;
                            retrievalEventHandler.waitForError();
                            assertThatChainOfCauses(retrievalEventHandler.getError())
                                    .anySatisfy(t -> assertThat(t).hasMessageContaining(errMsg));
                        });
            }
        };
    }

    @Test
    void testKubernetesLeaderRetrievalOnModified() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderRetrievalConfigMapCallback();

                            // Leader changed
                            final String newLeader = LEADER_ADDRESS + "_" + 2;
                            putLeaderInformationIntoConfigMap(UUID.randomUUID(), newLeader);

                            callbackHandler.onModified(
                                    Collections.singletonList(getLeaderConfigMap()));

                            assertThat(retrievalEventHandler.waitForNewLeader())
                                    .isEqualTo(newLeader);
                        });
            }
        };
    }

    @Test
    void testKubernetesLeaderRetrievalOnModifiedWithEmpty() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderRetrievalConfigMapCallback();

                            // Leader information is cleared
                            getLeaderConfigMap().getData().clear();
                            callbackHandler.onModified(
                                    Collections.singletonList(getLeaderConfigMap()));
                            retrievalEventHandler.waitForEmptyLeaderInformation();
                            assertThat(retrievalEventHandler.getAddress()).isNull();
                        });
            }
        };
    }
}
