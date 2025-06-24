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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InternalOperatorCoordinatorMetricGroup}. */
class InternalOperatorCoordinatorGroupTest {

    private static final MetricRegistry registry = TestingMetricRegistry.builder().build();

    @Test
    void testGenerateScopeDefault() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");
        InternalOperatorCoordinatorMetricGroup operatorCoordinatorMetricGroup =
                new InternalOperatorCoordinatorMetricGroup(jmJobGroup);

        assertThat(operatorCoordinatorMetricGroup.getScopeComponents())
                .containsExactly("localhost", "jobmanager", "myJobName", "opName", "coordinator");
        assertThat(operatorCoordinatorMetricGroup.getMetricIdentifier("name"))
                .isEqualTo("localhost.jobmanager.myJobName.opName.coordinator.name");
    }

    @Test
    void testVariables() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "host")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");
        InternalOperatorCoordinatorMetricGroup operatorCoordinatorMetricGroup =
                new InternalOperatorCoordinatorMetricGroup(jmJobGroup);

        Map<String, String> variables = operatorCoordinatorMetricGroup.getAllVariables();

        testVariable(variables, ScopeFormat.SCOPE_HOST, "host");
        testVariable(variables, ScopeFormat.SCOPE_JOB_ID, jobId.toString());
        testVariable(variables, ScopeFormat.SCOPE_JOB_NAME, "myJobName");
        testVariable(variables, ScopeFormat.SCOPE_TASK_VERTEX_ID, jobVertexId.toString());
        testVariable(variables, ScopeFormat.SCOPE_TASK_NAME, "taskName");
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_ID, operatorId.toString());
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_NAME, "opName");
    }

    private static void testVariable(
            Map<String, String> variables, String key, String expectedValue) {
        String actualValue = variables.get(key);
        assertThat(actualValue).isNotNull();
        assertThat(actualValue).isEqualTo(expectedValue);
    }
}
