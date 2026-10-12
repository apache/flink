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
import org.apache.flink.runtime.rest.messages.job.rescales.JobIDRescaleIDParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails.VertexParallelismRescaleInfo;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.rescales.SchedulerStateSpan;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescaleIdInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummary;
import org.apache.flink.runtime.scheduler.adaptive.timeline.SlotSharingGroupRescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminatedReason;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TriggerCause;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails.fromRescale;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JobRescaleDetailsHandler}. */
class JobRescaleDetailsHandlerTest {

    private final JobRescaleDetailsHandler testInstance =
            new JobRescaleDetailsHandler(
                    CompletableFuture::new,
                    TestingUtils.TIMEOUT,
                    Map.of(),
                    JobRescaleDetailsHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor());

    @Test
    void testUnNormalCases() throws HandlerRequestException, RestHandlerException {
        // Test for adaptive scheduler rescales was not enabled for job.
        final ExecutionGraphInfo executionGraphInfoWithNullRescalesStatsSnapshot =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(), List.of(), null);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(
                        executionGraphInfoWithNullRescalesStatsSnapshot.getJobId(),
                        new RescaleIdInfo.RescaleUUID());
        assertThatThrownBy(
                        () ->
                                testInstance.handleRequest(
                                        request, executionGraphInfoWithNullRescalesStatsSnapshot))
                .isInstanceOf(RestHandlerException.class);

        // Test for that case could not find rescale details for the specified rescale uuid.
        final ExecutionGraphInfo executionGraphInfoWithEmptyRescalesStatsSnapshot =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(),
                        List.of(),
                        JobManagerOptions.SchedulerType.Adaptive,
                        null,
                        RescalesStatsSnapshot.emptySnapshot());
        assertThatThrownBy(
                        () ->
                                testInstance.handleRequest(
                                        request, executionGraphInfoWithEmptyRescalesStatsSnapshot))
                .isInstanceOf(RestHandlerException.class);
    }

    @Test
    void testRequestNormalJobRescaleStatisticsDetails()
            throws HandlerRequestException, RestHandlerException {
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

        final ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(),
                        List.of(),
                        null,
                        null,
                        new RescalesStatsSnapshot(
                                List.of(rescale),
                                Map.of(rescale.getTerminalState(), rescale),
                                rescalesSummary.createSnapshot()));
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(
                        executionGraphInfo.getJobId(), rescale.getRescaleIdInfo().getRescaleUuid());
        JobRescaleDetails jobRescaleDetails =
                testInstance.handleRequest(request, executionGraphInfo);
        assertThat(jobRescaleDetails).isEqualTo(fromRescale(rescale, true));
    }

    @Test
    void testRequestLatestRescaleEvictedFromHistory()
            throws HandlerRequestException, RestHandlerException {
        final Rescale latestIgnoredRescale =
                createTerminatedRescale(TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
        final Rescale retainedCompletedRescale =
                createTerminatedRescale(TerminatedReason.SUCCEEDED);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(latestIgnoredRescale, retainedCompletedRescale);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(
                        executionGraphInfo.getJobId(),
                        latestIgnoredRescale.getRescaleIdInfo().getRescaleUuid());

        assertThat(testInstance.handleRequest(request, executionGraphInfo))
                .isEqualTo(fromRescale(latestIgnoredRescale, true));
    }

    @Test
    void testRequestHistoryOnlyRescale() throws HandlerRequestException, RestHandlerException {
        final Rescale latestIgnoredRescale =
                createTerminatedRescale(TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
        final Rescale retainedCompletedRescale =
                createTerminatedRescale(TerminatedReason.SUCCEEDED);
        final Rescale historyOnlyFailedRescale =
                createTerminatedRescale(TerminatedReason.EXCEPTION_OCCURRED);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(
                        latestIgnoredRescale,
                        retainedCompletedRescale,
                        List.of(historyOnlyFailedRescale, retainedCompletedRescale));
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(
                        executionGraphInfo.getJobId(),
                        historyOnlyFailedRescale.getRescaleIdInfo().getRescaleUuid());

        assertThat(testInstance.handleRequest(request, executionGraphInfo))
                .isEqualTo(fromRescale(historyOnlyFailedRescale, true));
    }

    @Test
    void testRequestUnknownRescale() throws HandlerRequestException {
        final Rescale latestIgnoredRescale =
                createTerminatedRescale(TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
        final Rescale retainedCompletedRescale =
                createTerminatedRescale(TerminatedReason.SUCCEEDED);
        final Rescale historyOnlyFailedRescale =
                createTerminatedRescale(TerminatedReason.EXCEPTION_OCCURRED);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(
                        latestIgnoredRescale,
                        retainedCompletedRescale,
                        List.of(historyOnlyFailedRescale, retainedCompletedRescale));
        final RescaleIdInfo.RescaleUUID unknownRescaleUuid = new RescaleIdInfo.RescaleUUID();
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId(), unknownRescaleUuid);

        assertThatThrownBy(() -> testInstance.handleRequest(request, executionGraphInfo))
                .isInstanceOfSatisfying(
                        RestHandlerException.class,
                        exception -> {
                            assertThat(exception.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.NOT_FOUND);
                            assertThat(exception.getMessage())
                                    .contains(unknownRescaleUuid.toString());
                        });
    }

    @Test
    void testArchiveIncludesLatestRescaleEvictedFromHistory() throws IOException {
        final Rescale latestIgnoredRescale =
                createTerminatedRescale(TerminatedReason.RESOURCE_REQUIREMENTS_UPDATED);
        final Rescale retainedCompletedRescale =
                createTerminatedRescale(TerminatedReason.SUCCEEDED);
        final Rescale historyOnlyFailedRescale =
                createTerminatedRescale(TerminatedReason.EXCEPTION_OCCURRED);
        final ExecutionGraphInfo executionGraphInfo =
                createExecutionGraphInfo(
                        latestIgnoredRescale,
                        retainedCompletedRescale,
                        List.of(historyOnlyFailedRescale, retainedCompletedRescale));

        final Collection<ArchivedJson> archives =
                testInstance.archiveJsonWithPath(executionGraphInfo);

        assertThat(archives)
                .extracting(ArchivedJson::getPath)
                .containsExactlyInAnyOrder(
                        getDetailsPath(executionGraphInfo, latestIgnoredRescale),
                        getDetailsPath(executionGraphInfo, retainedCompletedRescale),
                        getDetailsPath(executionGraphInfo, historyOnlyFailedRescale));
    }

    private static ExecutionGraphInfo createExecutionGraphInfo(
            Rescale latestIgnoredRescale, Rescale retainedCompletedRescale) {
        return createExecutionGraphInfo(
                latestIgnoredRescale, retainedCompletedRescale, List.of(retainedCompletedRescale));
    }

    private static ExecutionGraphInfo createExecutionGraphInfo(
            Rescale latestIgnoredRescale,
            Rescale retainedCompletedRescale,
            List<Rescale> rescaleHistory) {
        final RescalesSummary rescalesSummary = new RescalesSummary(1);
        rescalesSummary.addTerminated(latestIgnoredRescale);
        rescalesSummary.addTerminated(retainedCompletedRescale);
        return new ExecutionGraphInfo(
                new ArchivedExecutionGraphBuilder().build(),
                List.of(),
                null,
                null,
                new RescalesStatsSnapshot(
                        rescaleHistory,
                        Map.of(
                                latestIgnoredRescale.getTerminalState(),
                                latestIgnoredRescale,
                                retainedCompletedRescale.getTerminalState(),
                                retainedCompletedRescale),
                        rescalesSummary.createSnapshot()));
    }

    private static Rescale createTerminatedRescale(TerminatedReason terminatedReason) {
        return new Rescale(new RescaleIdInfo(new RescaleIdInfo.ResourceRequirementsID(), 1L))
                .setStartTimestamp(1L)
                .setEndTimestamp(2L)
                .setTriggerCause(TriggerCause.UPDATE_REQUIREMENT)
                .setTerminatedReason(terminatedReason);
    }

    private static String getDetailsPath(ExecutionGraphInfo executionGraphInfo, Rescale rescale) {
        return JobRescaleDetailsHeaders.getInstance()
                .getTargetRestEndpointURL()
                .replace(':' + JobIDPathParameter.KEY, executionGraphInfo.getJobId().toString())
                .replace(
                        ':' + JobRescaleIDPathParameter.KEY,
                        rescale.getRescaleIdInfo().getRescaleUuid().toString());
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(
            JobID jobId, RescaleIdInfo.RescaleUUID rescaleUuid) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());
        pathParameters.put(JobRescaleIDPathParameter.KEY, rescaleUuid.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobIDRescaleIDParameters(),
                pathParameters,
                new HashMap<>(),
                List.of());
    }
}
