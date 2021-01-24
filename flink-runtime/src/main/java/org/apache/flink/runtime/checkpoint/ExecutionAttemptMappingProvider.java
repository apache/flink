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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides a mapping from {@link ExecutionAttemptID} to {@link ExecutionVertex} for currently
 * running execution attempts.
 */
public class ExecutionAttemptMappingProvider {

    /** A full list of tasks. */
    private final List<ExecutionVertex> tasks;

    /** The cached mapping, which would only be updated on miss. */
    private final LinkedHashMap<ExecutionAttemptID, ExecutionVertex> cachedTasksById;

    public ExecutionAttemptMappingProvider(Iterable<ExecutionVertex> tasksIterable) {
        this.tasks = new ArrayList<>();
        tasksIterable.forEach(this.tasks::add);

        this.cachedTasksById =
                new LinkedHashMap<ExecutionAttemptID, ExecutionVertex>(tasks.size()) {

                    @Override
                    protected boolean removeEldestEntry(
                            Map.Entry<ExecutionAttemptID, ExecutionVertex> eldest) {
                        return size() > tasks.size();
                    }
                };
    }

    public Optional<ExecutionVertex> getVertex(ExecutionAttemptID id) {
        if (!cachedTasksById.containsKey(id)) {
            cachedTasksById.putAll(getCurrentAttemptMappings());
            if (!cachedTasksById.containsKey(id)) {
                // the task probably gone after a restart
                cachedTasksById.put(id, null);
            }
        }
        return Optional.ofNullable(cachedTasksById.get(id));
    }

    private Map<ExecutionAttemptID, ExecutionVertex> getCurrentAttemptMappings() {
        Map<ExecutionAttemptID, ExecutionVertex> attemptMappings = new HashMap<>(tasks.size());
        for (ExecutionVertex task : tasks) {
            attemptMappings.put(task.getCurrentExecutionAttempt().getAttemptId(), task);
        }

        return attemptMappings;
    }
}
