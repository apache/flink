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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionConfigBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.util.NoOpExecutionGraphCache;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link CheckpointConfigHandler} correctly exposes regional checkpoint fields. */
class CheckpointConfigHandlerRegionalTest {

    @Test
    void testRegionalCheckpointEnabledFields() throws Exception {
        final CheckpointCoordinatorConfiguration coordinatorConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(1000L)
                        .setCheckpointTimeout(60000L)
                        .setMinPauseBetweenCheckpoints(500L)
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointRetentionPolicy(
                                CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION)
                        .setExactlyOnce(true)
                        .setTolerableCheckpointFailureNumber(3)
                        .setUnalignedCheckpointsEnabled(false)
                        .setAlignedCheckpointTimeout(0L)
                        .setEnableCheckpointsAfterTasksFinish(true)
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.5)
                        .setRegionalMaxConsecutiveFailures(5)
                        .build();

        final CheckpointConfigInfo result = invokeHandler(coordinatorConfig);

        assertThat(result.isRegionalCheckpointEnabled()).isTrue();
        assertThat(result.getRegionalMaxFailureRatio()).isEqualTo(0.5);
        assertThat(result.getRegionalMaxConsecutiveFailures()).isEqualTo(5);
    }

    @Test
    void testRegionalCheckpointDisabledFields() throws Exception {
        final CheckpointCoordinatorConfiguration coordinatorConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(1000L)
                        .setCheckpointTimeout(60000L)
                        .setMinPauseBetweenCheckpoints(500L)
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointRetentionPolicy(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)
                        .setExactlyOnce(false)
                        .setTolerableCheckpointFailureNumber(0)
                        .setUnalignedCheckpointsEnabled(false)
                        .setAlignedCheckpointTimeout(0L)
                        .setEnableCheckpointsAfterTasksFinish(false)
                        .setRegionalCheckpointEnabled(false)
                        .setRegionalMaxFailureRatio(0.3)
                        .setRegionalMaxConsecutiveFailures(2)
                        .build();

        final CheckpointConfigInfo result = invokeHandler(coordinatorConfig);

        assertThat(result.isRegionalCheckpointEnabled()).isFalse();
        assertThat(result.getRegionalMaxFailureRatio()).isEqualTo(0.3);
        assertThat(result.getRegionalMaxConsecutiveFailures()).isEqualTo(2);
    }

    private CheckpointConfigInfo invokeHandler(CheckpointCoordinatorConfiguration coordinatorConfig)
            throws Exception {
        final JobID jobId = new JobID();
        final AccessExecutionGraph executionGraph =
                createArchivedExecutionGraph(jobId, coordinatorConfig);

        final GatewayRetriever<RestfulGateway> leaderRetriever =
                () -> CompletableFuture.completedFuture(null);

        final CheckpointConfigHandler handler =
                new CheckpointConfigHandler(
                        leaderRetriever,
                        Duration.ofSeconds(10),
                        Collections.emptyMap(),
                        CheckpointConfigHeaders.getInstance(),
                        NoOpExecutionGraphCache.INSTANCE,
                        Executors.newSingleThreadExecutor());

        final Map<String, String> pathParams = new HashMap<>();
        pathParams.put(JobIDPathParameter.KEY, jobId.toString());

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new JobMessageParameters(),
                        pathParams,
                        Collections.emptyMap(),
                        Collections.emptyList());

        return handler.handleRequest(request, executionGraph);
    }

    private static ArchivedExecutionGraph createArchivedExecutionGraph(
            JobID jobId, CheckpointCoordinatorConfiguration coordinatorConfig) {
        return new ArchivedExecutionGraph(
                jobId,
                "testJob",
                Collections
                        .<org.apache.flink.runtime.jobgraph.JobVertexID, ArchivedExecutionJobVertex>
                                emptyMap(),
                new ArrayList<>(),
                new long[JobStatus.values().length],
                JobStatus.RUNNING,
                JobType.STREAMING,
                null,
                new JobPlanInfo.Plan(jobId.toString(), "testJob", "", new ArrayList<>()),
                new StringifiedAccumulatorResult[0],
                Collections.emptyMap(),
                new ArchivedExecutionConfigBuilder().build(),
                false,
                coordinatorConfig,
                null,
                "hashmap",
                "jobmanager",
                TernaryBoolean.FALSE,
                null,
                null,
                0,
                null);
    }
}
