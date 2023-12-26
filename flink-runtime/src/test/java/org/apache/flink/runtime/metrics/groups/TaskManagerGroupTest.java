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
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.AbstractID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskManagerMetricGroup}. */
class TaskManagerGroupTest {

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
    //  adding and removing jobs
    // ------------------------------------------------------------------------

    @Test
    void addAndRemoveJobs() {
        final TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "localhost", ResourceID.generate());

        final JobID jid1 = new JobID();
        final JobID jid2 = new JobID();

        final String jobName1 = "testjob";
        final String jobName2 = "anotherJob";

        final JobVertexID vertex11 = new JobVertexID();
        final JobVertexID vertex12 = new JobVertexID();
        final JobVertexID vertex13 = new JobVertexID();
        final JobVertexID vertex21 = new JobVertexID();

        final ExecutionAttemptID execution11 = createExecutionAttemptId(vertex11, 17, 0);
        final ExecutionAttemptID execution12 = createExecutionAttemptId(vertex12, 13, 1);
        final ExecutionAttemptID execution13 = createExecutionAttemptId(vertex13, 0, 0);
        final ExecutionAttemptID execution21 = createExecutionAttemptId(vertex21, 7, 2);

        TaskMetricGroup tmGroup11 = group.addJob(jid1, jobName1).addTask(execution11, "test");

        TaskMetricGroup tmGroup12 = group.addJob(jid1, jobName1).addTask(execution12, "test");

        TaskMetricGroup tmGroup21 = group.addJob(jid2, jobName2).addTask(execution21, "test");

        assertThat(group.numRegisteredJobMetricGroups()).isEqualTo(2);
        assertThat(tmGroup11.parent().isClosed()).isFalse();
        assertThat(tmGroup12.parent().isClosed()).isFalse();
        assertThat(tmGroup21.parent().isClosed()).isFalse();

        // close all for job 2 and one from job 1
        tmGroup11.close();
        tmGroup21.close();
        assertThat(tmGroup11.isClosed()).isTrue();
        assertThat(tmGroup21.isClosed()).isTrue();

        // job 2 should be removed, job should still be there
        assertThat(tmGroup11.parent().isClosed()).isFalse();
        assertThat(tmGroup12.parent().isClosed()).isFalse();

        // should keep TaskManagerJobMetricGroup open - slot isn't released yet
        assertThat(tmGroup21.parent().isClosed()).isFalse();
        assertThat(group.numRegisteredJobMetricGroups()).isEqualTo(2);

        // add one more to job one

        TaskMetricGroup tmGroup13 = group.addJob(jid1, jobName1).addTask(execution13, "test");
        assertThat(tmGroup11.parent())
                .isSameAs(tmGroup13.parent()); // should use the same TaskManagerJobMetricGroup
        tmGroup12.close();
        tmGroup13.close();
    }

    @Test
    void testCloseClosesAll() {
        final TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "localhost", new ResourceID(new AbstractID().toString()));

        final JobID jid1 = new JobID();
        final JobID jid2 = new JobID();

        final String jobName1 = "testjob";
        final String jobName2 = "anotherJob";

        final JobVertexID vertex11 = new JobVertexID();
        final JobVertexID vertex12 = new JobVertexID();
        final JobVertexID vertex21 = new JobVertexID();

        final ExecutionAttemptID execution11 = createExecutionAttemptId(vertex11, 17, 0);
        final ExecutionAttemptID execution12 = createExecutionAttemptId(vertex12, 13, 1);
        final ExecutionAttemptID execution21 = createExecutionAttemptId(vertex21, 7, 1);

        TaskMetricGroup tmGroup11 = group.addJob(jid1, jobName1).addTask(execution11, "test");

        TaskMetricGroup tmGroup12 = group.addJob(jid1, jobName1).addTask(execution12, "test");

        TaskMetricGroup tmGroup21 = group.addJob(jid2, jobName2).addTask(execution21, "test");

        group.close();

        assertThat(tmGroup11.isClosed()).isTrue();
        assertThat(tmGroup12.isClosed()).isTrue();
        assertThat(tmGroup21.isClosed()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  scope name tests
    // ------------------------------------------------------------------------

    @Test
    void testGenerateScopeDefault() {
        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "localhost", new ResourceID("id"));

        assertThat(group.getScopeComponents()).containsExactly("localhost", "taskmanager", "id");
        assertThat(group.getMetricIdentifier("name")).isEqualTo("localhost.taskmanager.id.name");
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TM, "constant.<host>.foo.<host>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));
        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        assertThat(group.getScopeComponents()).containsExactly("constant", "host", "foo", "host");
        assertThat(group.getMetricIdentifier("name")).isEqualTo("constant.host.foo.host.name");
        registry.closeAsync().get();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        QueryScopeInfo.TaskManagerQueryScopeInfo info =
                tm.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.taskManagerID).isEqualTo("id");
    }
}
