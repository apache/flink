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

package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for grouping the TaskExecutorGateway and the InstanceID of a registered
 * task executor.
 */
public class TaskExecutorConnection {

    private final ResourceID resourceID;

    private final InstanceID instanceID;

    private final TaskExecutorGateway taskExecutorGateway;

    public TaskExecutorConnection(ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
        this.resourceID = checkNotNull(resourceID);
        this.instanceID = new InstanceID();
        this.taskExecutorGateway = checkNotNull(taskExecutorGateway);
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    public InstanceID getInstanceID() {
        return instanceID;
    }

    public TaskExecutorGateway getTaskExecutorGateway() {
        return taskExecutorGateway;
    }
}
