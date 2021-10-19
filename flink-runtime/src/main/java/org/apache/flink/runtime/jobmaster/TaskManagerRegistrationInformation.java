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

import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.UUID;

/** DTO for TaskManager registration information. */
public class TaskManagerRegistrationInformation implements Serializable {
    private static final long serialVersionUID = 1767026305134276540L;

    private final String taskManagerRpcAddress;
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;
    private final UUID taskManagerSession;

    private TaskManagerRegistrationInformation(
            String taskManagerRpcAddress,
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
            UUID taskManagerSession) {
        this.taskManagerRpcAddress = Preconditions.checkNotNull(taskManagerRpcAddress);
        this.unresolvedTaskManagerLocation =
                Preconditions.checkNotNull(unresolvedTaskManagerLocation);
        this.taskManagerSession = Preconditions.checkNotNull(taskManagerSession);
    }

    public String getTaskManagerRpcAddress() {
        return taskManagerRpcAddress;
    }

    public UnresolvedTaskManagerLocation getUnresolvedTaskManagerLocation() {
        return unresolvedTaskManagerLocation;
    }

    public UUID getTaskManagerSession() {
        return taskManagerSession;
    }

    public static TaskManagerRegistrationInformation create(
            String taskManagerRpcAddress,
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
            UUID taskManagerSession) {
        return new TaskManagerRegistrationInformation(
                taskManagerRpcAddress, unresolvedTaskManagerLocation, taskManagerSession);
    }
}
