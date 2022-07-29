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

package org.apache.flink.runtime.executiongraph;

/**
 * {@link ArchivedSpeculativeExecutionVertex} is a readonly representation of {@link
 * SpeculativeExecutionVertex}.
 */
public class ArchivedSpeculativeExecutionVertex extends ArchivedExecutionVertex {

    private static final long serialVersionUID = 1L;

    public ArchivedSpeculativeExecutionVertex(SpeculativeExecutionVertex vertex) {
        super(
                vertex.getParallelSubtaskIndex(),
                vertex.getTaskNameWithSubtaskIndex(),
                vertex.getCurrentExecutionAttempt().archive(),
                getCopyOfExecutionHistory(vertex));
    }

    private static ExecutionHistory getCopyOfExecutionHistory(SpeculativeExecutionVertex vertex) {
        final ExecutionHistory executionHistory =
                ArchivedExecutionVertex.getCopyOfExecutionHistory(vertex);

        // add all the executions to the execution history except for the only admitted current
        // execution
        final Execution currentAttempt = vertex.getCurrentExecutionAttempt();
        for (Execution execution : vertex.getCurrentExecutions()) {
            if (execution.getAttemptNumber() != currentAttempt.getAttemptNumber()) {
                executionHistory.add(execution.archive());
            }
        }

        return executionHistory;
    }
}
