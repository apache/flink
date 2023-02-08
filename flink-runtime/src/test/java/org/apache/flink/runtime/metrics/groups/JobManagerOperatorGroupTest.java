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
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link JobManagerOperatorMetricGroup}. */
public class JobManagerOperatorGroupTest {
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
            registry.closeAsync().get();
        }
    }

    // ------------------------------------------------------------------------
    //  adding and removing operators
    // ------------------------------------------------------------------------

    @Test
    public void addOperators() throws Exception {
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

        assertEquals(jmOperatorGroup11, jmOperatorGroup12);

        assertEquals(2, jmJobGroup.numRegisteredOperatorMetricGroups());

        jmOperatorGroup11.close();
        assertTrue(jmOperatorGroup11.isClosed());
        assertEquals(1, jmJobGroup.numRegisteredOperatorMetricGroups());

        jmOperatorGroup21.close();
        assertTrue(jmOperatorGroup21.isClosed());
        assertEquals(0, jmJobGroup.numRegisteredOperatorMetricGroups());
    }

    @Test
    public void testCloseClosesAll() throws Exception {
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

        assertEquals(jmOperatorGroup11, jmOperatorGroup12);
        assertEquals(2, jmJobGroup.numRegisteredOperatorMetricGroups());

        jmJobGroup.close();

        assertTrue(jmOperatorGroup11.isClosed());
        assertTrue(jmOperatorGroup21.isClosed());
    }

    // ------------------------------------------------------------------------
    //  scope name tests
    // ------------------------------------------------------------------------

    @Test
    public void testGenerateScopeDefault() throws Exception {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertArrayEquals(
                new String[] {"localhost", "jobmanager", "myJobName", "opName"},
                jmJobGroup.getScopeComponents());
        assertEquals(
                "localhost.jobmanager.myJobName.opName.name",
                jmJobGroup.getMetricIdentifier("name"));
    }

    @Test
    public void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(
                MetricOptions.SCOPE_NAMING_JM_OPERATOR,
                "constant.<host>.foo.<host>.<job_id>.<job_name>.<task_id>.<task_name>.<operator_id>.<operator_name>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertArrayEquals(
                new String[] {
                    "constant",
                    "host",
                    "foo",
                    "host",
                    jobId.toString(),
                    "myJobName",
                    jobVertexId.toString(),
                    "taskName",
                    operatorId.toString(),
                    "opName"
                },
                jmJobGroup.getScopeComponents());
        assertEquals(
                String.format(
                        "constant.host.foo.host.%s.myJobName.%s.taskName.%s.opName.name",
                        jobId, jobVertexId, operatorId),
                jmJobGroup.getMetricIdentifier("name"));

        registry.closeAsync().get();
    }

    @Test
    public void testGenerateScopeCustomWildcard() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "peter");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "*.some-constant.<job_id>");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_OPERATOR, "*.other-constant.<operator_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));

        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        assertArrayEquals(
                new String[] {
                    "peter",
                    "some-constant",
                    jobId.toString(),
                    "other-constant",
                    operatorId.toString()
                },
                jmJobGroup.getScopeComponents());

        assertEquals(
                String.format("peter.some-constant.%s.other-constant.%s.name", jobId, operatorId),
                jmJobGroup.getMetricIdentifier("name"));

        registry.closeAsync().get();
    }

    @Test
    public void testCreateQueryServiceMetricInfo() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");

        QueryScopeInfo.JobQueryScopeInfo info =
                jmJobGroup.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertEquals("", info.scope);
        assertEquals(jobId.toString(), info.jobID);
    }
}
