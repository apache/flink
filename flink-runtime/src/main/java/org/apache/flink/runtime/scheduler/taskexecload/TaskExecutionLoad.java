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

package org.apache.flink.runtime.scheduler.taskexecload;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Abstraction describing the amount of {@code task execution} assigned to a scheduling target.
 *
 * <p>This is a scheduler-facing metric that supports comparing and aggregating placement choices.
 * It is <b>not</b> a direct measurement of runtime resource pressure (for example CPU or memory
 * utilization) on a {@code TaskExecutor}.
 *
 * <p>During scheduling, a load value may be associated with different scheduler concepts on both
 * sides of a placement decision:
 *
 * <ul>
 *   <li><b>Resource requests</b> (for example slot-sharing related groupings in the {@code
 *       AdaptiveScheduler} or pending requests in the default scheduler)
 *   <li><b>Resource entities</b> (for example {@code TaskManager} instances)
 * </ul>
 */
@Internal
public interface TaskExecutionLoad extends Comparable<TaskExecutionLoad>, Serializable {

    /**
     * Returns the task execution load value.
     *
     * @return the current task execution load value
     */
    float getLoadValue();

    /**
     * Returns the task execution load value as an integer, truncated if necessary.
     *
     * @return the current resource unit count
     */
    int getLoadValueAsInt();

    /**
     * Merge another task execution load and this one into a new object.
     *
     * @param other another task execution load object
     * @return the new merged {@link TaskExecutionLoad}.
     */
    TaskExecutionLoad merge(TaskExecutionLoad other);
}
