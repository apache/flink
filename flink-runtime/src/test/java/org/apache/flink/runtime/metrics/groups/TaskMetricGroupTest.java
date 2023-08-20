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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskMetricGroup}. */
class TaskMetricGroupTest {

    private MetricRegistryImpl registry;

    @BeforeEach
    void setup() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
    }

    @AfterEach
    void teardown() throws Exception {
        if (registry != null) {
            registry.closeAsync().get();
        }
    }

    // ------------------------------------------------------------------------
    //  scope tests
    // -----------------------------------------------------------------------

    @Test
    void testGenerateScopeDefault() {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName")
                        .addTask(createExecutionAttemptId(new JobVertexID(), 13, 2), "aTaskName");

        assertThat(taskGroup.getScopeComponents())
                .containsExactly(
                        "theHostName", "taskmanager", "test-tm-id", "myJobName", "aTaskName", "13");

        assertThat(taskGroup.getMetricIdentifier("name"))
                .isEqualTo("theHostName.taskmanager.test-tm-id.myJobName.aTaskName.13.name");
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TM, "abc");
        cfg.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "def");
        cfg.setString(
                MetricOptions.SCOPE_NAMING_TASK, "<tm_id>.<job_id>.<task_id>.<task_attempt_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobID jid = new JobID();
        JobVertexID vertexId = new JobVertexID();
        ExecutionAttemptID executionId = createExecutionAttemptId(vertexId, 13, 2);

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(jid, "myJobName").addTask(executionId, "aTaskName");

        assertThat(taskGroup.getScopeComponents())
                .containsExactly(
                        "test-tm-id", jid.toString(), vertexId.toString(), executionId.toString());

        assertThat(taskGroup.getMetricIdentifier("name"))
                .isEqualTo(String.format("test-tm-id.%s.%s.%s.name", jid, vertexId, executionId));
        registry.closeAsync().get();
    }

    @Test
    void testGenerateScopeWilcard() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TASK, "*.<task_attempt_id>.<subtask_index>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        ExecutionAttemptID executionId = createExecutionAttemptId(new JobVertexID(), 13, 1);

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName").addTask(executionId, "aTaskName");

        assertThat(taskGroup.getScopeComponents())
                .containsExactly(
                        "theHostName",
                        "taskmanager",
                        "test-tm-id",
                        "myJobName",
                        executionId.toString(),
                        "13");

        assertThat(taskGroup.getMetricIdentifier("name"))
                .isEqualTo(
                        "theHostName.taskmanager.test-tm-id.myJobName." + executionId + ".13.name");
        registry.closeAsync().get();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        JobVertexID vid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(vid, 4, 5);
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        TaskMetricGroup task = tm.addJob(jid, "jobname").addTask(eid, "taskName");

        QueryScopeInfo.TaskQueryScopeInfo info =
                task.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo(jid.toString());
        assertThat(info.vertexID).isEqualTo(vid.toString());
        assertThat(info.subtaskIndex).isEqualTo(4);
    }

    @Test
    void testTaskMetricGroupCleanup() throws Exception {
        CountingMetricRegistry registry = new CountingMetricRegistry(new Configuration());
        TaskManagerMetricGroup taskManagerMetricGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "localhost", new ResourceID("0"));

        int initialMetricsCount = registry.getNumberRegisteredMetrics();
        TaskMetricGroup taskMetricGroup =
                taskManagerMetricGroup
                        .addJob(new JobID(), "job")
                        .addTask(createExecutionAttemptId(), "task");

        // the io metric should have registered predefined metrics
        assertThat(registry.getNumberRegisteredMetrics()).isGreaterThan(initialMetricsCount);

        taskMetricGroup.close();

        // now all registered metrics should have been unregistered
        assertThat(registry.getNumberRegisteredMetrics()).isEqualTo(initialMetricsCount);

        registry.closeAsync().get();
    }

    @Test
    void testOperatorNameTruncation() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_OPERATOR, ScopeFormat.SCOPE_OPERATOR_NAME);
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        TaskMetricGroup taskMetricGroup =
                tm.addJob(new JobID(), "jobname").addTask(createExecutionAttemptId(), "task");

        String originalName = new String(new char[100]).replace("\0", "-");
        InternalOperatorMetricGroup operatorMetricGroup =
                taskMetricGroup.getOrAddOperator(originalName);

        String storedName = operatorMetricGroup.getScopeComponents()[0];
        assertThat(storedName.length()).isEqualTo(TaskMetricGroup.METRICS_OPERATOR_NAME_MAX_LENGTH);
        assertThat(originalName.substring(0, TaskMetricGroup.METRICS_OPERATOR_NAME_MAX_LENGTH))
                .isEqualTo(storedName);
        registry.closeAsync().get();
    }

    private static class CountingMetricRegistry extends MetricRegistryImpl {

        private int counter = 0;

        CountingMetricRegistry(Configuration config) {
            super(MetricRegistryTestUtils.fromConfiguration(config));
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            super.register(metric, metricName, group);
            counter++;
        }

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
            super.unregister(metric, metricName, group);
            counter--;
        }

        int getNumberRegisteredMetrics() {
            return counter;
        }
    }
}
