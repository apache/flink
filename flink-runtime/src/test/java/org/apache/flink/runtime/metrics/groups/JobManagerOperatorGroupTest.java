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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JobManagerOperatorMetricGroup}. */
class JobManagerOperatorGroupTest {
    private static final MetricRegistry registry = TestingMetricRegistry.builder().build();

    // ------------------------------------------------------------------------
    //  adding and removing operators
    // ------------------------------------------------------------------------

    @Test
    void addOperators() {
        JobManagerJobMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(new JobID(), "myJobName");

        final JobVertexID jobVertexId1 = new JobVertexID();
        final JobVertexID jobVertexId2 = new JobVertexID();
        final OperatorID operatorId1 = new OperatorID();
        final OperatorID operatorId2 = new OperatorID();

        JobManagerOperatorMetricGroup jmOperatorGroup11 =
                jmJobGroup.getOrAddOperator(jobVertexId1, "taskName1", operatorId1, "opName1");
        JobManagerOperatorMetricGroup jmOperatorGroup12 =
                jmJobGroup.getOrAddOperator(jobVertexId1, "taskName1", operatorId1, "opName1");
        JobManagerOperatorMetricGroup jmOperatorGroup21 =
                jmJobGroup.getOrAddOperator(jobVertexId2, "taskName3", operatorId2, "opName3");

        assertThat(jmOperatorGroup11).isEqualTo(jmOperatorGroup12);

        assertThat(jmJobGroup.numRegisteredOperatorMetricGroups()).isEqualTo(2);

        jmOperatorGroup11.close();
        assertThat(jmOperatorGroup11.isClosed()).isTrue();
        assertThat(jmJobGroup.numRegisteredOperatorMetricGroups()).isOne();

        jmOperatorGroup21.close();
        assertThat(jmOperatorGroup21.isClosed()).isTrue();
        assertThat(jmJobGroup.numRegisteredOperatorMetricGroups()).isZero();
    }

    @Test
    void testCloseClosesAll() {
        JobManagerJobMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "theHostName")
                        .addJob(new JobID(), "myJobName");

        final JobVertexID jobVertexId1 = new JobVertexID();
        final JobVertexID jobVertexId2 = new JobVertexID();
        final OperatorID operatorId1 = new OperatorID();
        final OperatorID operatorId2 = new OperatorID();

        JobManagerOperatorMetricGroup jmOperatorGroup11 =
                jmJobGroup.getOrAddOperator(jobVertexId1, "taskName1", operatorId1, "opName1");
        JobManagerOperatorMetricGroup jmOperatorGroup12 =
                jmJobGroup.getOrAddOperator(jobVertexId1, "taskName1", operatorId1, "opName1");
        JobManagerOperatorMetricGroup jmOperatorGroup21 =
                jmJobGroup.getOrAddOperator(jobVertexId2, "taskName3", operatorId2, "opName3");

        assertThat(jmOperatorGroup11).isEqualTo(jmOperatorGroup12);
        assertThat(jmJobGroup.numRegisteredOperatorMetricGroups()).isEqualTo(2);

        jmJobGroup.close();

        assertThat(jmOperatorGroup11.isClosed()).isTrue();
        assertThat(jmOperatorGroup21.isClosed()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  scope name tests
    // ------------------------------------------------------------------------

    @Test
    void testGenerateScopeDefault() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertThat(jmJobGroup.getScopeComponents())
                .containsExactly("localhost", "jobmanager", "myJobName", "opName");
        assertThat(jmJobGroup.getMetricIdentifier("name"))
                .isEqualTo("localhost.jobmanager.myJobName.opName.name");
    }

    @Test
    void testGenerateScopeCustom() {
        Configuration cfg = new Configuration();
        cfg.setString(
                MetricOptions.SCOPE_NAMING_JM_OPERATOR,
                "constant.<host>.foo.<host>.<job_id>.<job_name>.<task_id>.<task_name>.<operator_id>.<operator_name>");
        MetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setScopeFormats(ScopeFormats.fromConfig(cfg))
                        .build();

        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertThat(jmJobGroup.getScopeComponents())
                .containsExactly(
                        "constant",
                        "host",
                        "foo",
                        "host",
                        jobId.toString(),
                        "myJobName",
                        jobVertexId.toString(),
                        "taskName",
                        operatorId.toString(),
                        "opName");
        assertThat(jmJobGroup.getMetricIdentifier("name"))
                .isEqualTo(
                        String.format(
                                "constant.host.foo.host.%s.myJobName.%s.taskName.%s.opName.name",
                                jobId, jobVertexId, operatorId));
    }

    @Test
    void testGenerateScopeCustomWildcard() {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "peter");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "*.some-constant.<job_id>");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_OPERATOR, "*.other-constant.<operator_id>");
        MetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setScopeFormats(ScopeFormats.fromConfig(cfg))
                        .build();

        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertThat(jmJobGroup.getScopeComponents())
                .containsExactly(
                        "peter",
                        "some-constant",
                        jobId.toString(),
                        "other-constant",
                        operatorId.toString());

        assertThat(jmJobGroup.getMetricIdentifier("name"))
                .isEqualTo(
                        String.format(
                                "peter.some-constant.%s.other-constant.%s.name",
                                jobId, operatorId));
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        QueryScopeInfo.JobManagerOperatorQueryScopeInfo info =
                jmJobGroup.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo(jobId.toString());
        assertThat(info.vertexID).isEqualTo(jobVertexId.toString());
        assertThat(info.operatorName).isEqualTo("opName");
    }
}
