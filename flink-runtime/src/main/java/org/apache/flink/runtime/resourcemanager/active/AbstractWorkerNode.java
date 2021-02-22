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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;

/** Represents a worker in {@link ActiveResourceManager}. */
public abstract class AbstractWorkerNode implements ResourceIDRetrievable {
    private final ResourceID resourceId;
    private final Optional<TaskExecutorProcessSpec> taskExecutorProcessSpecOpt;

    protected AbstractWorkerNode(
            ResourceID resourceId, @Nullable TaskExecutorProcessSpec taskExecutorProcessSpec) {
        this.resourceId = Preconditions.checkNotNull(resourceId);
        this.taskExecutorProcessSpecOpt = Optional.ofNullable(taskExecutorProcessSpec);
    }

    @Override
    public ResourceID getResourceID() {
        return resourceId;
    }

    public Optional<TaskExecutorProcessSpec> getTaskExecutorProcessSpec() {
        return taskExecutorProcessSpecOpt;
    }
}
