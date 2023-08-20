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
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JobManagerJobMetricGroup}. */
class JobManagerJobGroupTest {

    @Test
    void testGenerateScopeDefault() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        JobManagerJobMetricGroup jmGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(new JobID(), "myJobName");

        assertThat(jmGroup.getScopeComponents())
                .containsExactly("theHostName", "jobmanager", "myJobName");

        assertThat(jmGroup.getMetricIdentifier("name"))
                .isEqualTo("theHostName.jobmanager.myJobName.name");

        registry.closeAsync().get();
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "abc");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "some-constant.<job_name>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobManagerJobMetricGroup jmGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(new JobID(), "myJobName");

        assertThat(jmGroup.getScopeComponents()).containsExactly("some-constant", "myJobName");

        assertThat(jmGroup.getMetricIdentifier("name")).isEqualTo("some-constant.myJobName.name");

        registry.closeAsync().get();
    }

    @Test
    void testGenerateScopeCustomWildcard() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "peter");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "*.some-constant.<job_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobID jid = new JobID();

        JobManagerJobMetricGroup jmGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(jid, "myJobName");

        assertThat(jmGroup.getScopeComponents())
                .containsExactly("peter", "some-constant", jid.toString());

        assertThat(jmGroup.getMetricIdentifier("name"))
                .isEqualTo("peter.some-constant." + jid + ".name");

        registry.closeAsync().get();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        JobManagerJobMetricGroup jmj =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(jid, "myJobName");

        QueryScopeInfo.JobQueryScopeInfo info =
                jmj.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo(jid.toString());
    }
}
