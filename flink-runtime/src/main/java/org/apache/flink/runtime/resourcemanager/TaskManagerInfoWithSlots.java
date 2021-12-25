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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/** Contains the base information about a {@link TaskExecutor} and its allocated slots. */
public class TaskManagerInfoWithSlots implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TaskManagerInfo taskManagerInfo;
    private final Collection<SlotInfo> allocatedSlots;

    public TaskManagerInfoWithSlots(
            TaskManagerInfo taskManagerInfo, Collection<SlotInfo> allocatedSlots) {
        this.taskManagerInfo = Preconditions.checkNotNull(taskManagerInfo);
        this.allocatedSlots = Preconditions.checkNotNull(allocatedSlots);
    }

    public final Collection<SlotInfo> getAllocatedSlots() {
        return Collections.unmodifiableCollection(allocatedSlots);
    }

    public TaskManagerInfo getTaskManagerInfo() {
        return taskManagerInfo;
    }
}
