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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RestartAllFailoverStrategy}. */
class RestartAllFailoverStrategyTest {

    @Test
    void testGetTasksNeedingRestart() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
        final TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
        final TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();

        topology.connect(v1, v2, ResultPartitionType.PIPELINED);
        topology.connect(v2, v3, ResultPartitionType.BLOCKING);

        final RestartAllFailoverStrategy strategy = new RestartAllFailoverStrategy(topology);

        assertThat(new HashSet<>(Arrays.asList(v1.getId(), v2.getId(), v3.getId())))
                .isEqualTo(
                        strategy.getTasksNeedingRestart(v1.getId(), new Exception("Test failure")));
    }
}
