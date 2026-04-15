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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails.VertexParallelismRescaleInfo;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesHistory;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesHistoryHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.SchedulerStateSpan;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescaleIdInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummary;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummarySnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.SlotSharingGroupRescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminatedReason;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TriggerCause;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails.fromRescale;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JobRescalesHistoryHandler}. */
class JobRescalesHistoryHandlerTest {
    private final JobRescalesHistoryHandler testInstance =
            new JobRescalesHistoryHandler(
                    CompletableFuture::new,
                    TestingUtils.TIMEOUT,
                    Collections.emptyMap(),
                    JobRescalesHistoryHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor());

    @Test
    void testSchedulerNotEnabledRescalesHistory() throws HandlerRequestException {
        // Test for adaptive scheduler rescales was not enabled for job.
        final ExecutionGraphInfo executionGraphInfoWithNullRescalesStatsSnapshot =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(), Collections.emptyList(), null);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfoWithNullRescalesStatsSnapshot.getJobId());
        assertThatThrownBy(
                        () ->
                                testInstance.handleRequest(
                                        request, executionGraphInfoWithNullRescalesStatsSnapshot))
                .isInstanceOf(RestHandlerException.class);
    }

    @Test
    void testRequestNormalJobRescaleHistory() throws HandlerRequestException, RestHandlerException {
        Rescale rescale =
                new Rescale(new RescaleIdInfo(new RescaleIdInfo.ResourceRequirementsID(), 1L))
                        .setStartTimestamp(1L)
                        .setEndTimestamp(100L)
                        .setTriggerCause(TriggerCause.INITIAL_SCHEDULE)
                        .setStringifiedException("mocked exception")
                        .addSchedulerState(new SchedulerStateSpan("Created", 1L, 5L, 4L, null))
                        .setTerminatedReason(TerminatedReason.SUCCEEDED);

        JobVertexID jobVertexID = new JobVertexID();
        SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

        SlotSharingGroupRescale slotSharingGroupRescale =
                new SlotSharingGroupRescale(slotSharingGroup);
        slotSharingGroupRescale.setPostRescaleSlots(2);
        slotSharingGroupRescale.setPreRescaleSlots(1);
        slotSharingGroupRescale.setDesiredSlots(5);
        slotSharingGroupRescale.setMinimalRequiredSlots(1);
        slotSharingGroupRescale.setAcquiredResourceProfile(ResourceProfile.ZERO);

        VertexParallelismRescaleInfo vertexParallelismRescaleInfo =
                new VertexParallelismRescaleInfo(
                        jobVertexID,
                        "jvName",
                        slotSharingGroup.getSlotSharingGroupId(),
                        "default",
                        5,
                        1,
                        1,
                        2);

        rescale.getModifiableSlots()
                .put(slotSharingGroup.getSlotSharingGroupId(), slotSharingGroupRescale);
        rescale.getModifiableVertices().put(jobVertexID, vertexParallelismRescaleInfo);

        RescalesSummary rescalesSummary = new RescalesSummary(2);
        rescalesSummary.addTerminated(rescale);
        RescalesSummarySnapshot rescalesSummarySnapshot = rescalesSummary.createSnapshot();
        RescalesStatsSnapshot rescalesStatsSnapshot =
                new RescalesStatsSnapshot(
                        Collections.singletonList(rescale),
                        Collections.singletonMap(rescale.getTerminalState(), rescale),
                        rescalesSummarySnapshot);

        final ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(),
                        Collections.emptyList(),
                        JobManagerOptions.SchedulerType.Adaptive,
                        null,
                        rescalesStatsSnapshot);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId());
        JobRescalesHistory actualJobRescalesHistory =
                testInstance.handleRequest(request, executionGraphInfo);

        JobRescalesHistory expectedJobRescalesHistory =
                JobRescalesHistory.from(Collections.singletonList(fromRescale(rescale, false)));
        assertThat(actualJobRescalesHistory).isEqualTo(expectedJobRescalesHistory);
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(JobID jobId)
            throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobMessageParameters(),
                pathParameters,
                new HashMap<>(),
                Collections.emptyList());
    }
}
