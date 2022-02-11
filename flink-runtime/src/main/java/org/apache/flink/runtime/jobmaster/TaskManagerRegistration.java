/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.UUID;

final class TaskManagerRegistration {
    private final TaskManagerLocation taskManagerLocation;
    private final TaskExecutorGateway taskExecutorGateway;
    private final UUID sessionId;

    private TaskManagerRegistration(
            TaskManagerLocation taskManagerLocation,
            TaskExecutorGateway taskExecutorGateway,
            UUID sessionId) {
        this.taskManagerLocation = taskManagerLocation;
        this.taskExecutorGateway = taskExecutorGateway;
        this.sessionId = sessionId;
    }

    TaskManagerLocation getTaskManagerLocation() {
        return taskManagerLocation;
    }

    TaskExecutorGateway getTaskExecutorGateway() {
        return taskExecutorGateway;
    }

    UUID getSessionId() {
        return sessionId;
    }

    static TaskManagerRegistration create(
            TaskManagerLocation taskManagerLocation,
            TaskExecutorGateway taskExecutorGateway,
            UUID sessionId) {
        return new TaskManagerRegistration(taskManagerLocation, taskExecutorGateway, sessionId);
    }
}
