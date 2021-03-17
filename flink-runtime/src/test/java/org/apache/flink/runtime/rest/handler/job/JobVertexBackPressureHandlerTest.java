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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.MetricDump.GaugeDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo.TaskQueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.SubtaskBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.VertexBackPressureStatus;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH;
import static org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.VertexBackPressureLevel.LOW;
import static org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.VertexBackPressureLevel.OK;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link JobVertexBackPressureHandler}. */
public class JobVertexBackPressureHandlerTest {

    /** Job ID for which back pressure stats exist. */
    private static final JobID TEST_JOB_ID_BACK_PRESSURE_STATS_AVAILABLE = new JobID();

    private static final JobVertexID TEST_JOB_VERTEX_ID = new JobVertexID();

    /** Job ID for which back pressure stats are not available. */
    private static final JobID TEST_JOB_ID_BACK_PRESSURE_STATS_ABSENT = new JobID();

    private TestingRestfulGateway restfulGateway;

    private JobVertexBackPressureHandler jobVertexBackPressureHandler;

    private MetricStore metricStore;

    private static Collection<MetricDump> getMetricDumps() {
        Collection<MetricDump> dumps = new ArrayList<>();
        TaskQueryScopeInfo task0 =
                new TaskQueryScopeInfo(
                        TEST_JOB_ID_BACK_PRESSURE_STATS_AVAILABLE.toString(),
                        TEST_JOB_VERTEX_ID.toString(),
                        0);
        dumps.add(new GaugeDump(task0, MetricNames.TASK_BACK_PRESSURED_TIME, "1000"));
        dumps.add(new GaugeDump(task0, MetricNames.TASK_IDLE_TIME, "0"));
        dumps.add(new GaugeDump(task0, MetricNames.TASK_BUSY_TIME, "0"));

        TaskQueryScopeInfo task1 =
                new TaskQueryScopeInfo(
                        TEST_JOB_ID_BACK_PRESSURE_STATS_AVAILABLE.toString(),
                        TEST_JOB_VERTEX_ID.toString(),
                        1);
        dumps.add(new GaugeDump(task1, MetricNames.TASK_BACK_PRESSURED_TIME, "500"));
        dumps.add(new GaugeDump(task1, MetricNames.TASK_IDLE_TIME, "100"));
        dumps.add(new GaugeDump(task1, MetricNames.TASK_BUSY_TIME, "900"));

        // missing task2

        TaskQueryScopeInfo task3 =
                new TaskQueryScopeInfo(
                        TEST_JOB_ID_BACK_PRESSURE_STATS_AVAILABLE.toString(),
                        TEST_JOB_VERTEX_ID.toString(),
                        3);
        dumps.add(new GaugeDump(task3, MetricNames.TASK_BACK_PRESSURED_TIME, "100"));
        dumps.add(new GaugeDump(task3, MetricNames.TASK_IDLE_TIME, "200"));
        dumps.add(new GaugeDump(task3, MetricNames.TASK_BUSY_TIME, "700"));

        return dumps;
    }

    @Before
    public void setUp() {
        metricStore = new MetricStore();
        for (MetricDump metricDump : getMetricDumps()) {
            metricStore.add(metricDump);
        }

        jobVertexBackPressureHandler =
                new JobVertexBackPressureHandler(
                        () -> CompletableFuture.completedFuture(restfulGateway),
                        Time.seconds(10),
                        Collections.emptyMap(),
                        JobVertexBackPressureHeaders.getInstance(),
                        new MetricFetcher() {
                            private long updateCount = 0;

                            @Override
                            public MetricStore getMetricStore() {
                                return metricStore;
                            }

                            @Override
                            public void update() {
                                updateCount++;
                            }

                            @Override
                            public long getLastUpdateTime() {
                                return updateCount;
                            }
                        });
    }

    @Test
    public void testGetBackPressure() throws Exception {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(
                JobIDPathParameter.KEY, TEST_JOB_ID_BACK_PRESSURE_STATS_AVAILABLE.toString());
        pathParameters.put(JobVertexIdPathParameter.KEY, TEST_JOB_VERTEX_ID.toString());

        final HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        new JobVertexMessageParameters(),
                        pathParameters,
                        Collections.emptyMap());

        final CompletableFuture<JobVertexBackPressureInfo>
                jobVertexBackPressureInfoCompletableFuture =
                        jobVertexBackPressureHandler.handleRequest(request, restfulGateway);
        final JobVertexBackPressureInfo jobVertexBackPressureInfo =
                jobVertexBackPressureInfoCompletableFuture.get();

        assertThat(jobVertexBackPressureInfo.getStatus(), equalTo(VertexBackPressureStatus.OK));
        assertThat(jobVertexBackPressureInfo.getBackpressureLevel(), equalTo(HIGH));

        assertThat(
                jobVertexBackPressureInfo.getSubtasks().stream()
                        .map(SubtaskBackPressureInfo::getBackPressuredRatio)
                        .collect(Collectors.toList()),
                contains(1.0, 0.5, 0.1));

        assertThat(
                jobVertexBackPressureInfo.getSubtasks().stream()
                        .map(SubtaskBackPressureInfo::getIdleRatio)
                        .collect(Collectors.toList()),
                contains(0.0, 0.1, 0.2));

        assertThat(
                jobVertexBackPressureInfo.getSubtasks().stream()
                        .map(SubtaskBackPressureInfo::getBusyRatio)
                        .collect(Collectors.toList()),
                contains(0.0, 0.9, 0.7));

        assertThat(
                jobVertexBackPressureInfo.getSubtasks().stream()
                        .map(SubtaskBackPressureInfo::getBackpressureLevel)
                        .collect(Collectors.toList()),
                contains(HIGH, LOW, OK));

        assertThat(
                jobVertexBackPressureInfo.getSubtasks().stream()
                        .map(SubtaskBackPressureInfo::getSubtask)
                        .collect(Collectors.toList()),
                contains(0, 1, 3));
    }

    @Test
    public void testAbsentBackPressure() throws Exception {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(
                JobIDPathParameter.KEY, TEST_JOB_ID_BACK_PRESSURE_STATS_ABSENT.toString());
        pathParameters.put(JobVertexIdPathParameter.KEY, new JobVertexID().toString());

        final HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        new JobVertexMessageParameters(),
                        pathParameters,
                        Collections.emptyMap());

        final CompletableFuture<JobVertexBackPressureInfo>
                jobVertexBackPressureInfoCompletableFuture =
                        jobVertexBackPressureHandler.handleRequest(request, restfulGateway);
        final JobVertexBackPressureInfo jobVertexBackPressureInfo =
                jobVertexBackPressureInfoCompletableFuture.get();

        assertThat(
                jobVertexBackPressureInfo.getStatus(),
                equalTo(VertexBackPressureStatus.DEPRECATED));
    }
}
