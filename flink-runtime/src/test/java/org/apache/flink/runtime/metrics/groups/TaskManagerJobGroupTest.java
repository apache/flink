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
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskManagerJobMetricGroup}. */
class TaskManagerJobGroupTest {

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
    void testGenerateScopeDefault() {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));
        JobMetricGroup jmGroup =
                new TaskManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");

        assertThat(jmGroup.getScopeComponents())
                .containsExactly("theHostName", "taskmanager", "test-tm-id", "myJobName");

        assertThat(jmGroup.getMetricIdentifier("name"))
                .isEqualTo("theHostName.taskmanager.test-tm-id.myJobName.name");
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TM, "abc");
        cfg.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "some-constant.<job_name>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobID jid = new JobID();

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));
        JobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

        assertThat(jmGroup.getScopeComponents()).containsExactly("some-constant", "myJobName");

        assertThat(jmGroup.getMetricIdentifier("name")).isEqualTo("some-constant.myJobName.name");
        registry.closeAsync().get();
    }

    @Test
    void testGenerateScopeCustomWildcard() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_TM, "peter.<tm_id>");
        cfg.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "*.some-constant.<job_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobID jid = new JobID();

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));
        JobMetricGroup jmGroup = new TaskManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

        assertThat(jmGroup.getScopeComponents())
                .containsExactly("peter", "test-tm-id", "some-constant", jid.toString());

        assertThat(jmGroup.getMetricIdentifier("name"))
                .isEqualTo("peter.test-tm-id.some-constant." + jid + ".name");
        registry.closeAsync().get();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        TaskManagerJobMetricGroup job = new TaskManagerJobMetricGroup(registry, tm, jid, "jobname");

        QueryScopeInfo.JobQueryScopeInfo info =
                job.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo(jid.toString());
    }
}
