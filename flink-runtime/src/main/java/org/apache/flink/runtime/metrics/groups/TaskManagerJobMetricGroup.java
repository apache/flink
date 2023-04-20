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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.metrics.MetricRegistry;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.holdsLock;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to a
 * specific job, running on the TaskManager.
 *
 * <p>Contains extra logic for adding Tasks ({@link TaskMetricGroup}).
 */
@Internal
public class TaskManagerJobMetricGroup extends JobMetricGroup<TaskManagerMetricGroup> {

    /** Map from execution attempt ID (task identifier) to task metrics. */
    private final Map<ExecutionAttemptID, TaskMetricGroup> tasks = new HashMap<>();

    // ------------------------------------------------------------------------

    public TaskManagerJobMetricGroup(
            MetricRegistry registry,
            TaskManagerMetricGroup parent,
            JobID jobId,
            @Nullable String jobName) {
        super(
                registry,
                parent,
                jobId,
                jobName,
                registry.getScopeFormats()
                        .getTaskManagerJobFormat()
                        .formatScope(checkNotNull(parent), jobId, jobName));
    }

    public final TaskManagerMetricGroup parent() {
        return parent;
    }

    // ------------------------------------------------------------------------
    //  adding / removing tasks
    // ------------------------------------------------------------------------

    public TaskMetricGroup addTask(
            final ExecutionAttemptID executionAttemptID, final String taskName) {
        checkNotNull(executionAttemptID);
        checkNotNull(taskName);

        synchronized (this) {
            if (!isClosed()) {
                TaskMetricGroup prior = tasks.get(executionAttemptID);
                if (prior != null) {
                    return prior;
                } else {
                    TaskMetricGroup task =
                            new TaskMetricGroup(registry, this, executionAttemptID, taskName);
                    tasks.put(executionAttemptID, task);
                    return task;
                }
            } else {
                return null;
            }
        }
    }

    public void removeTaskMetricGroup(ExecutionAttemptID executionId) {
        checkNotNull(executionId);

        // this can be a call from this.close which iterates over tasks
        // changing tasks here would break iteration
        synchronized (this) {
            if (!isClosed()) {
                tasks.remove(executionId);
                // keep this group open even if tasks is empty - to re-use on new task submission
                // the group will be closed by TM with the release of the last job slot on this TM
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        checkState(holdsLock(this));
        return tasks.values();
    }
}
