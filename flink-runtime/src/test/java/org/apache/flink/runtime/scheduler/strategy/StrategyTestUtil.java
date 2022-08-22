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

package org.apache.flink.runtime.scheduler.strategy;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Strategy test utilities. */
class StrategyTestUtil {

    static void assertLatestScheduledVerticesAreEqualTo(
            final List<List<TestingSchedulingExecutionVertex>> expected,
            TestingSchedulerOperations testingSchedulerOperation) {
        final List<List<ExecutionVertexID>> allScheduledVertices =
                testingSchedulerOperation.getScheduledVertices();
        final int expectedScheduledBulks = expected.size();
        assertThat(expectedScheduledBulks).isLessThanOrEqualTo(allScheduledVertices.size());
        for (int i = 0; i < expectedScheduledBulks; i++) {
            assertThat(allScheduledVertices.get(allScheduledVertices.size() - i - 1))
                    .isEqualTo(idsFromVertices(expected.get(expectedScheduledBulks - i - 1)));
        }
    }

    static List<ExecutionVertexID> idsFromVertices(
            final List<TestingSchedulingExecutionVertex> vertices) {
        return vertices.stream()
                .map(TestingSchedulingExecutionVertex::getId)
                .collect(Collectors.toList());
    }
}
