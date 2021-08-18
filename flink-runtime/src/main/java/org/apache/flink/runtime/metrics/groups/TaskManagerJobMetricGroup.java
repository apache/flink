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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

    TaskManagerJobMetricGroup(
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
            final JobVertexID jobVertexId,
            final ExecutionAttemptID executionAttemptID,
            final String taskName,
            final int subtaskIndex,
            final int attemptNumber) {
        checkNotNull(jobVertexId);
        checkNotNull(executionAttemptID);
        checkNotNull(taskName);

        synchronized (this) {
            if (!isClosed()) {
                TaskMetricGroup prior = tasks.get(executionAttemptID);
                if (prior != null) {
                    return prior;
                } else {
                    TaskMetricGroup task =
                            new TaskMetricGroup(
                                    registry,
                                    this,
                                    jobVertexId,
                                    executionAttemptID,
                                    taskName,
                                    subtaskIndex,
                                    attemptNumber);
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

        boolean removeFromParent = false;
        synchronized (this) {
            if (!isClosed() && tasks.remove(executionId) != null && tasks.isEmpty()) {
                // this call removed the last task. close this group.
                removeFromParent = true;
                close();
            }
        }

        // IMPORTANT: removing from the parent must not happen while holding the this group's lock,
        //      because it would violate the "first parent then subgroup" lock acquisition order
        if (removeFromParent) {
            parent.removeJobMetricsGroup(jobId, this);
        }
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return tasks.values();
    }
}
