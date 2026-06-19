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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import java.util.ArrayList;
import java.util.List;

/** Contains the results of the {@link ResourceAllocationStrategy}. */
public class ResourceReconcileResult {

    private final List<PendingTaskManager> pendingTaskManagersToAllocate;
    private final List<PendingTaskManager> pendingTaskManagersToRelease;
    private final List<TaskManagerInfo> taskManagersToRelease;

    public ResourceReconcileResult(
            List<PendingTaskManager> pendingTaskManagersToAllocate,
            List<PendingTaskManager> pendingTaskManagersToRelease,
            List<TaskManagerInfo> taskManagersToRelease) {
        this.pendingTaskManagersToAllocate = pendingTaskManagersToAllocate;
        this.pendingTaskManagersToRelease = pendingTaskManagersToRelease;
        this.taskManagersToRelease = taskManagersToRelease;
    }

    public List<PendingTaskManager> getPendingTaskManagersToAllocate() {
        return pendingTaskManagersToAllocate;
    }

    public List<PendingTaskManager> getPendingTaskManagersToRelease() {
        return pendingTaskManagersToRelease;
    }

    public List<TaskManagerInfo> getTaskManagersToRelease() {
        return taskManagersToRelease;
    }

    /**
     * Returns whether the cluster resource need reconcile.
     *
     * @return True if the cluster resource need reconcile, otherwise false.
     */
    public boolean needReconcile() {
        return pendingTaskManagersToRelease.size() > 0
                || taskManagersToRelease.size() > 0
                || pendingTaskManagersToAllocate.size() > 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<PendingTaskManager> pendingTaskManagersToAllocate = new ArrayList<>();
        private final List<PendingTaskManager> pendingTaskManagersToRelease = new ArrayList<>();
        private final List<TaskManagerInfo> taskManagersToRelease = new ArrayList<>();

        public Builder addPendingTaskManagerToAllocate(PendingTaskManager pendingTaskManager) {
            this.pendingTaskManagersToAllocate.add(pendingTaskManager);
            return this;
        }

        public Builder addPendingTaskManagerToRelease(PendingTaskManager pendingTaskManager) {
            this.pendingTaskManagersToRelease.add(pendingTaskManager);
            return this;
        }

        public Builder addTaskManagerToRelease(TaskManagerInfo taskManagerInfo) {
            this.taskManagersToRelease.add(taskManagerInfo);
            return this;
        }

        public ResourceReconcileResult build() {
            return new ResourceReconcileResult(
                    pendingTaskManagersToAllocate,
                    pendingTaskManagersToRelease,
                    taskManagersToRelease);
        }
    }
}
