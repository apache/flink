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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesOverview;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesSummary;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesSummaryHeaders;
import org.apache.flink.runtime.rest.messages.util.stats.StatsSummaryDto;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescaleIdInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummary;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesSummarySnapshot;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminatedReason;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TriggerCause;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JobRescalesSummaryHandler}. */
class JobRescalesSummaryHandlerTest {
    private final JobRescalesSummaryHandler testInstance =
            new JobRescalesSummaryHandler(
                    CompletableFuture::new,
                    TestingUtils.TIMEOUT,
                    Map.of(),
                    JobRescalesSummaryHeaders.getInstance(),
                    new DefaultExecutionGraphCache(TestingUtils.TIMEOUT, TestingUtils.TIMEOUT),
                    Executors.directExecutor());

    @Test
    void testSchedulerNotEnabledRescalesSummary() throws HandlerRequestException {
        final ExecutionGraphInfo executionGraphInfoWithNullRescalesStatsSnapshot =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(), List.of(), null);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfoWithNullRescalesStatsSnapshot.getJobId());
        assertThatThrownBy(
                        () ->
                                testInstance.handleRequest(
                                        request, executionGraphInfoWithNullRescalesStatsSnapshot))
                .isInstanceOf(RestHandlerException.class);
    }

    @Test
    void testRequestNormalJobRescaleSummary() throws HandlerRequestException, RestHandlerException {
        Rescale rescale =
                new Rescale(new RescaleIdInfo(new RescaleIdInfo.ResourceRequirementsID(), 1L))
                        .setStartTimestamp(1L)
                        .setEndTimestamp(100L)
                        .setTriggerCause(TriggerCause.INITIAL_SCHEDULE)
                        .setStringifiedException("mocked exception")
                        .setTerminatedReason(TerminatedReason.SUCCEEDED);

        RescalesSummary rescalesSummary = new RescalesSummary(2);
        rescalesSummary.addTerminated(rescale);
        RescalesSummarySnapshot rescalesSummarySnapshot = rescalesSummary.createSnapshot();
        RescalesStatsSnapshot rescalesStatsSnapshot =
                new RescalesStatsSnapshot(
                        List.of(rescale),
                        Map.of(rescale.getTerminalState(), rescale),
                        rescalesSummarySnapshot);

        final ExecutionGraphInfo executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().build(),
                        List.of(),
                        JobManagerOptions.SchedulerType.Adaptive,
                        null,
                        rescalesStatsSnapshot);
        final HandlerRequest<EmptyRequestBody> request =
                createRequest(executionGraphInfo.getJobId());

        JobRescalesSummary actual = testInstance.handleRequest(request, executionGraphInfo);
        JobRescalesSummary expected =
                new JobRescalesSummary(
                        new JobRescalesOverview.RescalesCounts(0L, 0L, 1L, 0L),
                        new StatsSummaryDto(99L, 99L, 99L, 99L, 99L, 99L, 99L, 99L),
                        new StatsSummaryDto(99L, 99L, 99L, 99L, 99L, 99L, 99L, 99L),
                        new StatsSummaryDto(
                                0L,
                                0L,
                                0L,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN),
                        new StatsSummaryDto(
                                0L,
                                0L,
                                0L,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                Double.NaN));
        assertThat(actual).isEqualTo(expected);
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
                List.of());
    }
}
