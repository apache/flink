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
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link KubernetesLeaderRetrievalDriver}. */
public class KubernetesLeaderRetrievalDriverTest extends KubernetesHighAvailabilityTestBase {

    @Test
    public void testErrorForwarding() throws Exception {
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
                            retrievalEventHandler.waitForError(TIMEOUT);
                            assertThat(
                                    retrievalEventHandler.getError(),
                                    FlinkMatchers.containsMessage(errMsg));
                        });
            }
        };
    }

    @Test
    public void testKubernetesLeaderRetrievalOnModified() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>
                                    callbackHandler = getLeaderRetrievalConfigMapCallback();

                            // Leader changed
                            final String newLeader = LEADER_URL + "_" + 2;
                            getLeaderConfigMap()
                                    .getData()
                                    .put(Constants.LEADER_ADDRESS_KEY, newLeader);
                            callbackHandler.onModified(
                                    Collections.singletonList(getLeaderConfigMap()));

                            assertThat(
                                    retrievalEventHandler.waitForNewLeader(TIMEOUT), is(newLeader));
                        });
            }
        };
    }

    @Test
    public void testKubernetesLeaderRetrievalOnModifiedWithEmpty() throws Exception {
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
                            assertThat(retrievalEventHandler.getAddress(), is(nullValue()));
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
                                    callbackHandler = getLeaderRetrievalConfigMapCallback();
                            callbackHandler.handleError(
                                    new KubernetesTooOldResourceVersionException(
                                            new Exception("too old resource version")));
                            // Verify the old watch is closed and a new one is created
                            assertThat(configMapWatches.size(), is(3));
                            // The three watches are [leader-election-watch,
                            // old-leader-retrieval-watch, new-leader-retrieval-watch]
                            assertThat(
                                    configMapWatches.stream()
                                            .map(
                                                    TestingFlinkKubeClient.MockKubernetesWatch
                                                            ::isClosed)
                                            .collect(Collectors.toList()),
                                    contains(false, true, false));
                        });
            }
        };
    }
}
