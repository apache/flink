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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KubernetesCheckpointIDCounter} operations. */
class KubernetesCheckpointIDCounterTest extends KubernetesHighAvailabilityTestBase {

    @Test
    void testGetAndIncrement() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.setCount(100L);
                            final long counter = checkpointIDCounter.getAndIncrement();
                            assertThat(counter).isEqualTo(100L);
                            assertThat(checkpointIDCounter.get()).isEqualTo(101L);
                        });
            }
        };
    }

    @Test
    void testShutdown() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.start();

                            checkpointIDCounter.setCount(100L);

                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(Constants.CHECKPOINT_COUNTER_KEY, "100");

                            checkpointIDCounter.shutdown(JobStatus.FINISHED).join();

                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(Constants.CHECKPOINT_COUNTER_KEY);
                        });
            }
        };
    }

    @Test
    void testShutdownForLocallyTerminatedJobStatus() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.start();

                            checkpointIDCounter.setCount(100L);

                            assertThat(getLeaderConfigMap().getData())
                                    .containsEntry(Constants.CHECKPOINT_COUNTER_KEY, "100");

                            checkpointIDCounter.shutdown(JobStatus.SUSPENDED).join();

                            assertThat(getLeaderConfigMap().getData())
                                    .containsKey(Constants.CHECKPOINT_COUNTER_KEY);
                        });
            }
        };
    }

    @Test
    void testIdempotentShutdown() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.start();

                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(Constants.CHECKPOINT_COUNTER_KEY);

                            checkpointIDCounter.shutdown(JobStatus.FINISHED).join();

                            assertThat(getLeaderConfigMap().getData())
                                    .doesNotContainKey(Constants.CHECKPOINT_COUNTER_KEY);

                            // a second shutdown should work without causing any errors
                            checkpointIDCounter.shutdown(JobStatus.FINISHED).join();
                        });
            }
        };
    }

    @Test
    void testShutdownFailureDueToMissingConfigMap() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.start();

                            // deleting the ConfigMap from outside of the CheckpointIDCounter while
                            // still using the counter (which is stored as an entry in the
                            // ConfigMap) causes an unexpected failure which we want to simulate
                            // here
                            flinkKubeClient.deleteConfigMap(LEADER_CONFIGMAP_NAME);
                            assertThatThrownBy(
                                            () ->
                                                    checkpointIDCounter
                                                            .shutdown(JobStatus.FINISHED)
                                                            .get())
                                    .satisfies(anyCauseMatches(ExecutionException.class));

                            // fixing the internal issue should make the shutdown succeed again
                            KubernetesUtils.createConfigMapIfItDoesNotExist(
                                    flinkKubeClient, LEADER_CONFIGMAP_NAME, getClusterId());
                            checkpointIDCounter.shutdown(JobStatus.FINISHED).get();
                        });
            }
        };
    }

    @Test
    void testGetAndIncrementWithNoLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.setCount(100L);

                            // lost leadership
                            getLeaderCallback().notLeader();

                            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);
                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            final String errMsg =
                                    "Failed to update ConfigMap "
                                            + LEADER_CONFIGMAP_NAME
                                            + " since "
                                            + "current KubernetesCheckpointIDCounter does not have the leadership.";
                            assertThatThrownBy(
                                            () -> checkpointIDCounter.getAndIncrement(),
                                            "We should get an exception when trying to GetAndIncrement no leadership.")
                                    .satisfies(anyCauseMatches(errMsg));
                        });
            }
        };
    }

    @Test
    void testSetAndGet() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
                            checkpointIDCounter.setCount(100L);
                            final long counter = checkpointIDCounter.get();
                            assertThat(counter).isEqualTo(100L);
                        });
            }
        };
    }

    @Test
    void testGetWhenConfigMapNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);

                            final String errMsg =
                                    "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist.";
                            assertThatThrownBy(
                                            () -> checkpointIDCounter.get(),
                                            "We should get an exception when trying to get checkpoint id counter but ConfigMap does not exist.")
                                    .satisfies(anyCauseMatches(errMsg));
                        });
            }
        };
    }

    @Test
    void testSetWithNoLeadershipShouldNotBeIssued() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesCheckpointIDCounter checkpointIDCounter =
                                    new KubernetesCheckpointIDCounter(
                                            flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);

                            checkpointIDCounter.setCount(2L);

                            // lost leadership
                            getLeaderCallback().notLeader();
                            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);

                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            checkpointIDCounter.setCount(100L);
                            assertThat(checkpointIDCounter.get()).isEqualTo(2L);
                        });
            }
        };
    }
}
