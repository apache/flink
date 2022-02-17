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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

import com.mashape.unirest.http.exceptions.UnirestException;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.metrics.prometheus.PrometheusReporterTest.createReporterSetup;
import static org.apache.flink.metrics.prometheus.PrometheusReporterTest.pollMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PrometheusReporter} that registers several instances of the same metric for
 * different subtasks.
 */
class PrometheusReporterTaskScopeTest {
    private static final String[] LABEL_NAMES = {
        "job_id",
        "task_id",
        "task_attempt_id",
        "host",
        "task_name",
        "task_attempt_num",
        "job_name",
        "tm_id",
        "subtask_index"
    };

    private static final String TASK_MANAGER_HOST = "taskManagerHostName";
    private static final String TASK_MANAGER_ID = "taskManagerId";
    private static final String JOB_NAME = "jobName";
    private static final String TASK_NAME = "taskName";
    private static final int ATTEMPT_NUMBER = 0;
    private static final int SUBTASK_INDEX_1 = 0;
    private static final int SUBTASK_INDEX_2 = 1;

    private final JobID jobId = new JobID();
    private final JobVertexID taskId1 = new JobVertexID();
    private final ExecutionAttemptID taskAttemptId1 = new ExecutionAttemptID();
    private final String[] labelValues1 = {
        jobId.toString(),
        taskId1.toString(),
        taskAttemptId1.toString(),
        TASK_MANAGER_HOST,
        TASK_NAME,
        "" + ATTEMPT_NUMBER,
        JOB_NAME,
        TASK_MANAGER_ID,
        "" + SUBTASK_INDEX_1
    };
    private final JobVertexID taskId2 = new JobVertexID();
    private final ExecutionAttemptID taskAttemptId2 = new ExecutionAttemptID();
    private final String[] labelValues2 = {
        jobId.toString(),
        taskId2.toString(),
        taskAttemptId2.toString(),
        TASK_MANAGER_HOST,
        TASK_NAME,
        "" + ATTEMPT_NUMBER,
        JOB_NAME,
        TASK_MANAGER_ID,
        "" + SUBTASK_INDEX_2
    };

    private TaskMetricGroup taskMetricGroup1;
    private TaskMetricGroup taskMetricGroup2;

    private MetricRegistryImpl registry;
    private PrometheusReporter reporter;

    @BeforeEach
    void setupReporter() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(createReporterSetup("test1", "9400-9500")));
        reporter = (PrometheusReporter) registry.getReporters().get(0);

        TaskManagerMetricGroup tmMetricGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, TASK_MANAGER_HOST, new ResourceID(TASK_MANAGER_ID));
        taskMetricGroup1 =
                tmMetricGroup
                        .addJob(jobId, JOB_NAME)
                        .addTask(
                                taskId1,
                                taskAttemptId1,
                                TASK_NAME,
                                SUBTASK_INDEX_1,
                                ATTEMPT_NUMBER);

        taskMetricGroup2 =
                tmMetricGroup
                        .addJob(jobId, JOB_NAME)
                        .addTask(
                                taskId2,
                                taskAttemptId2,
                                TASK_NAME,
                                SUBTASK_INDEX_2,
                                ATTEMPT_NUMBER);
    }

    @AfterEach
    void shutdownRegistry() throws Exception {
        if (registry != null) {
            registry.shutdown().get();
        }
    }

    @Test
    void countersCanBeAddedSeveralTimesIfTheyDifferInLabels() throws UnirestException {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        taskMetricGroup1.counter("my_counter", counter1);
        taskMetricGroup2.counter("my_counter", counter2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues1))
                .isEqualTo(1.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues2))
                .isEqualTo(2.);
    }

    @Test
    void gaugesCanBeAddedSeveralTimesIfTheyDifferInLabels() throws UnirestException {
        Gauge<Integer> gauge1 =
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return 3;
                    }
                };
        Gauge<Integer> gauge2 =
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return 4;
                    }
                };

        taskMetricGroup1.gauge("my_gauge", gauge1);
        taskMetricGroup2.gauge("my_gauge", gauge2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_gauge", LABEL_NAMES, labelValues1))
                .isEqualTo(3.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_gauge", LABEL_NAMES, labelValues2))
                .isEqualTo(4.);
    }

    @Test
    void metersCanBeAddedSeveralTimesIfTheyDifferInLabels() throws UnirestException {
        Meter meter = new TestMeter();

        taskMetricGroup1.meter("my_meter", meter);
        taskMetricGroup2.meter("my_meter", meter);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_meter", LABEL_NAMES, labelValues1))
                .isEqualTo(5.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_meter", LABEL_NAMES, labelValues2))
                .isEqualTo(5.);
    }

    @Test
    void histogramsCanBeAddedSeveralTimesIfTheyDifferInLabels() throws UnirestException {
        Histogram histogram = new TestHistogram();

        taskMetricGroup1.histogram("my_histogram", histogram);
        taskMetricGroup2.histogram("my_histogram", histogram);

        final String exportedMetrics = pollMetrics(reporter.getPort()).getBody();
        assertThat(exportedMetrics)
                .contains("subtask_index=\"0\",quantile=\"0.5\",} 0.5"); // histogram
        assertThat(exportedMetrics)
                .contains("subtask_index=\"1\",quantile=\"0.5\",} 0.5"); // histogram

        final String[] labelNamesWithQuantile = addToArray(LABEL_NAMES, "quantile");
        for (Double quantile : PrometheusReporter.HistogramSummaryProxy.QUANTILES) {
            assertThat(
                            CollectorRegistry.defaultRegistry.getSampleValue(
                                    "flink_taskmanager_job_task_my_histogram",
                                    labelNamesWithQuantile,
                                    addToArray(labelValues1, "" + quantile)))
                    .isEqualTo(quantile);
            assertThat(
                            CollectorRegistry.defaultRegistry.getSampleValue(
                                    "flink_taskmanager_job_task_my_histogram",
                                    labelNamesWithQuantile,
                                    addToArray(labelValues2, "" + quantile)))
                    .isEqualTo(quantile);
        }
    }

    @Test
    void removingSingleInstanceOfMetricDoesNotBreakOtherInstances() throws UnirestException {
        Counter counter1 = new SimpleCounter();
        counter1.inc(1);
        Counter counter2 = new SimpleCounter();
        counter2.inc(2);

        taskMetricGroup1.counter("my_counter", counter1);
        taskMetricGroup2.counter("my_counter", counter2);

        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues1))
                .isEqualTo(1.);
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues2))
                .isEqualTo(2.);

        taskMetricGroup2.close();
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues1))
                .isEqualTo(1.);

        taskMetricGroup1.close();
        assertThat(
                        CollectorRegistry.defaultRegistry.getSampleValue(
                                "flink_taskmanager_job_task_my_counter", LABEL_NAMES, labelValues1))
                .isNull();
    }

    private String[] addToArray(String[] array, String element) {
        final String[] labelNames = Arrays.copyOf(array, LABEL_NAMES.length + 1);
        labelNames[LABEL_NAMES.length] = element;
        return labelNames;
    }
}
