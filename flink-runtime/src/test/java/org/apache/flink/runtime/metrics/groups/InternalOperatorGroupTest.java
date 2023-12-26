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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InternalOperatorMetricGroup}. */
class InternalOperatorGroupTest {

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

    @Test
    void testGenerateScopeDefault() throws Exception {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName")
                        .addTask(createExecutionAttemptId(new JobVertexID(), 11, 0), "aTaskName");
        InternalOperatorMetricGroup opGroup =
                taskGroup.getOrAddOperator(new OperatorID(), "myOpName");

        assertThat(opGroup.getScopeComponents())
                .containsExactly(
                        "theHostName", "taskmanager", "test-tm-id", "myJobName", "myOpName", "11");

        assertThat(opGroup.getMetricIdentifier("name"))
                .isEqualTo("theHostName.taskmanager.test-tm-id.myJobName.myOpName.11.name");
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(
                MetricOptions.SCOPE_NAMING_OPERATOR,
                "<tm_id>.<job_id>.<task_id>.<operator_name>.<operator_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));
        try {
            String tmID = "test-tm-id";
            JobID jid = new JobID();
            JobVertexID vertexId = new JobVertexID();
            OperatorID operatorID = new OperatorID();
            String operatorName = "operatorName";

            InternalOperatorMetricGroup operatorGroup =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                    registry, "theHostName", new ResourceID(tmID))
                            .addJob(jid, "myJobName")
                            .addTask(createExecutionAttemptId(vertexId, 13, 2), "aTaskname")
                            .getOrAddOperator(operatorID, operatorName);

            assertThat(operatorGroup.getScopeComponents())
                    .containsExactly(
                            tmID,
                            jid.toString(),
                            vertexId.toString(),
                            operatorName,
                            operatorID.toString());

            assertThat(operatorGroup.getMetricIdentifier("name"))
                    .isEqualTo(
                            "%s.%s.%s.%s.%s.name", tmID, jid, vertexId, operatorName, operatorID);
        } finally {
            registry.closeAsync().get();
        }
    }

    @Test
    void testIOMetricGroupInstantiation() throws Exception {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName")
                        .addTask(createExecutionAttemptId(new JobVertexID(), 11, 0), "aTaskName");
        InternalOperatorMetricGroup opGroup =
                taskGroup.getOrAddOperator(new OperatorID(), "myOpName");

        assertThat(opGroup.getIOMetricGroup()).isNotNull();
        assertThat(opGroup.getIOMetricGroup().getNumRecordsInCounter()).isNotNull();
        assertThat(opGroup.getIOMetricGroup().getNumRecordsOutCounter()).isNotNull();
    }

    @Test
    void testVariables() {
        JobID jid = new JobID();
        JobVertexID tid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(tid, 11, 0);
        OperatorID oid = new OperatorID();

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup = tmGroup.addJob(jid, "myJobName").addTask(eid, "aTaskName");
        InternalOperatorMetricGroup opGroup = taskGroup.getOrAddOperator(oid, "myOpName");

        Map<String, String> variables = opGroup.getAllVariables();

        testVariable(variables, ScopeFormat.SCOPE_HOST, "theHostName");
        testVariable(variables, ScopeFormat.SCOPE_TASKMANAGER_ID, "test-tm-id");
        testVariable(variables, ScopeFormat.SCOPE_JOB_ID, jid.toString());
        testVariable(variables, ScopeFormat.SCOPE_JOB_NAME, "myJobName");
        testVariable(variables, ScopeFormat.SCOPE_TASK_VERTEX_ID, tid.toString());
        testVariable(variables, ScopeFormat.SCOPE_TASK_NAME, "aTaskName");
        testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_ID, eid.toString());
        testVariable(variables, ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, "11");
        testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, "0");
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_ID, oid.toString());
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_NAME, "myOpName");
    }

    private static void testVariable(
            Map<String, String> variables, String key, String expectedValue) {
        String actualValue = variables.get(key);
        assertThat(actualValue).isNotNull().isEqualTo(expectedValue);
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        JobVertexID vid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(vid, 4, 5);
        OperatorID oid = new OperatorID();
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        TaskMetricGroup task = tm.addJob(jid, "jobname").addTask(eid, "taskName");
        InternalOperatorMetricGroup operator = task.getOrAddOperator(oid, "operator");

        QueryScopeInfo.OperatorQueryScopeInfo info =
                operator.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo(jid.toString());
        assertThat(info.vertexID).isEqualTo(vid.toString());
        assertThat(info.subtaskIndex).isEqualTo(4);
        assertThat(info.operatorName).isEqualTo("operator");
    }
}
