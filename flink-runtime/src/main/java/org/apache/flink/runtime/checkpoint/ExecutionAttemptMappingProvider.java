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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Responses to query the current attempt for the tasks and provide the mapping from the execution
 * attempt id to its task.
 */
public class ExecutionAttemptMappingProvider {

    private final List<ExecutionVertex> tasks;

    public ExecutionAttemptMappingProvider(Iterable<ExecutionVertex> tasks) {
        this.tasks = new ArrayList<>();
        tasks.forEach(this.tasks::add);
    }

    public int getNumberOfTasks() {
        return tasks.size();
    }

    public Map<ExecutionAttemptID, ExecutionVertex> getCurrentAttemptMappings() {
        Map<ExecutionAttemptID, ExecutionVertex> attemptMappings = new HashMap<>(tasks.size());
        for (ExecutionVertex task : tasks) {
            attemptMappings.put(task.getCurrentExecutionAttempt().getAttemptId(), task);
        }

        return attemptMappings;
    }
}
