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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.List;
import java.util.Map;

/** Component responsible for assigning slots to a collection of {@link Execution}. */
public interface ExecutionSlotAllocator {

    /**
     * Allocate slots for the given executions.
     *
     * @param executionAttemptIds executions to allocate slots for
     * @return Map of slot assignments to the executions
     */
    Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds);

    /**
     * Cancel the ongoing slot request of the given {@link Execution}.
     *
     * @param executionAttemptId identifying the {@link Execution} of which the slot request should
     *     be canceled.
     */
    void cancel(ExecutionAttemptID executionAttemptId);
}
