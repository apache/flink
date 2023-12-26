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
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JobManagerMetricGroup}. */
class JobManagerGroupTest {

    // ------------------------------------------------------------------------
    //  adding and removing jobs
    // ------------------------------------------------------------------------

    @Test
    public void addAndRemoveJobs() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        final JobManagerMetricGroup group =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost");

        final JobID jid1 = new JobID();
        final JobID jid2 = new JobID();

        final String jobName1 = "testjob";
        final String jobName2 = "anotherJob";

        JobManagerJobMetricGroup jmJobGroup11 = group.addJob(jid1, jobName1);
        JobManagerJobMetricGroup jmJobGroup12 = group.addJob(jid1, jobName1);
        JobManagerJobMetricGroup jmJobGroup21 = group.addJob(jid2, jobName2);

        assertThat(jmJobGroup12).isEqualTo(jmJobGroup11);

        assertThat(group.numRegisteredJobMetricGroups()).isEqualTo(2);

        group.localCleanupAsync(jid1, Executors.directExecutor()).join();

        assertThat(jmJobGroup11.isClosed()).isTrue();
        assertThat(group.numRegisteredJobMetricGroups()).isOne();

        group.localCleanupAsync(jid2, Executors.directExecutor()).join();

        assertThat(jmJobGroup21.isClosed()).isTrue();
        assertThat(group.numRegisteredJobMetricGroups()).isZero();

        registry.closeAsync().get();
    }

    @Test
    void testCloseClosesAll() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        final JobManagerMetricGroup group =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost");

        final JobID jid1 = new JobID();
        final JobID jid2 = new JobID();

        final String jobName1 = "testjob";
        final String jobName2 = "anotherJob";

        JobManagerJobMetricGroup jmJobGroup11 = group.addJob(jid1, jobName1);
        JobManagerJobMetricGroup jmJobGroup21 = group.addJob(jid2, jobName2);

        group.close();

        assertThat(jmJobGroup11.isClosed()).isTrue();
        assertThat(jmJobGroup21.isClosed()).isTrue();

        registry.closeAsync().get();
    }

    // ------------------------------------------------------------------------
    //  scope name tests
    // ------------------------------------------------------------------------

    @Test
    void testGenerateScopeDefault() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        JobManagerMetricGroup group =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost");

        assertThat(group.getScopeComponents()).containsExactly("localhost", "jobmanager");
        assertThat(group.getMetricIdentifier("name")).isEqualTo("localhost.jobmanager.name");

        registry.closeAsync().get();
    }

    @Test
    void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "constant.<host>.foo.<host>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        JobManagerMetricGroup group =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host");

        assertThat(group.getScopeComponents()).containsExactly("constant", "host", "foo", "host");

        registry.closeAsync().get();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
        JobManagerMetricGroup jm =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host");

        QueryScopeInfo.JobManagerQueryScopeInfo info =
                jm.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
    }
}
