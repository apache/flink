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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code ExecutionGraphInfoTest} tests the proper initialization of {@link ExecutionGraphInfo}. */
public class ExecutionGraphInfoTest {

    @Test
    public void testExecutionGraphHistoryBeingDerivedFromFailedExecutionGraph() {
        final ArchivedExecutionGraph executionGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        new JobID(),
                        "test job name",
                        JobStatus.FAILED,
                        new RuntimeException("Expected RuntimeException"),
                        null,
                        System.currentTimeMillis());

        final ExecutionGraphInfo executionGraphInfo = new ExecutionGraphInfo(executionGraph);

        final ErrorInfo failureInfo =
                executionGraphInfo.getArchivedExecutionGraph().getFailureInfo();

        final RootExceptionHistoryEntry actualEntry =
                Iterables.getOnlyElement(executionGraphInfo.getExceptionHistory());

        assertThat(failureInfo).isNotNull();
        assertThat(failureInfo.getException()).isEqualTo(actualEntry.getException());
        assertThat(failureInfo.getTimestamp()).isEqualTo(actualEntry.getTimestamp());

        assertThat(actualEntry.isGlobal()).isTrue();
        assertThat(actualEntry.getFailingTaskName()).isNull();
        assertThat(actualEntry.getTaskManagerLocation()).isNull();
    }
}
