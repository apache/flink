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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link TaskManagerMetricGroup}. */
public class TaskManagerGroupTest extends TestLogger {

    private MetricRegistryImpl registry;

    @Before
    public void setup() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
    }

    @After
    public void teardown() throws Exception {
        if (registry != null) {
            registry.shutdown().get();
        }
    }

    // ------------------------------------------------------------------------
    //  adding and removing jobs
    // ------------------------------------------------------------------------

    @Test
    public void addAndRemoveJobs() throws IOException {
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

        assertEquals(2, group.numRegisteredJobMetricGroups());
        assertFalse(tmGroup11.parent().isClosed());
        assertFalse(tmGroup12.parent().isClosed());
        assertFalse(tmGroup21.parent().isClosed());

        // close all for job 2 and one from job 1
        tmGroup11.close();
        tmGroup21.close();
        assertTrue(tmGroup11.isClosed());
        assertTrue(tmGroup21.isClosed());

        // job 2 should be removed, job should still be there
        assertFalse(tmGroup11.parent().isClosed());
        assertFalse(tmGroup12.parent().isClosed());

        // should keep TaskManagerJobMetricGroup open - slot isn't released yet
        assertFalse(tmGroup21.parent().isClosed());
        assertEquals(2, group.numRegisteredJobMetricGroups());

        // add one more to job one

        TaskMetricGroup tmGroup13 = group.addJob(jid1, jobName1).addTask(execution13, "test");
        assertSame(
                tmGroup11.parent(),
                tmGroup13.parent()); // should use the same TaskManagerJobMetricGroup
        tmGroup12.close();
        tmGroup13.close();
    }

    @Test
    public void testCloseClosesAll() throws IOException {
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

        assertTrue(tmGroup11.isClosed());
        assertTrue(tmGroup12.isClosed());
        assertTrue(tmGroup21.isClosed());
    }

    // ------------------------------------------------------------------------
    //  scope name tests
    // ------------------------------------------------------------------------

    @Test
    public void testGenerateScopeDefault() {
        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "localhost", new ResourceID("id"));

        assertArrayEquals(
                new String[] {"localhost", "taskmanager", "id"}, group.getScopeComponents());
        assertEquals("localhost.taskmanager.id.name", group.getMetricIdentifier("name"));
    }

    @Test
    public void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TM, "constant.<host>.foo.<host>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));
        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        assertArrayEquals(
                new String[] {"constant", "host", "foo", "host"}, group.getScopeComponents());
        assertEquals("constant.host.foo.host.name", group.getMetricIdentifier("name"));
        registry.shutdown().get();
    }

    @Test
    public void testCreateQueryServiceMetricInfo() {
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        QueryScopeInfo.TaskManagerQueryScopeInfo info =
                tm.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertEquals("", info.scope);
        assertEquals("id", info.taskManagerID);
    }
}
