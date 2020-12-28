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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/** Container for the resource manager connection instances used by the {@link TaskExecutor}. */
class EstablishedResourceManagerConnection {

    @Nonnull private final ResourceManagerGateway resourceManagerGateway;

    @Nonnull private final ResourceID resourceManagerResourceId;

    @Nonnull private final InstanceID taskExecutorRegistrationId;

    EstablishedResourceManagerConnection(
            @Nonnull ResourceManagerGateway resourceManagerGateway,
            @Nonnull ResourceID resourceManagerResourceId,
            @Nonnull InstanceID taskExecutorRegistrationId) {
        this.resourceManagerGateway = resourceManagerGateway;
        this.resourceManagerResourceId = resourceManagerResourceId;
        this.taskExecutorRegistrationId = taskExecutorRegistrationId;
    }

    @Nonnull
    public ResourceManagerGateway getResourceManagerGateway() {
        return resourceManagerGateway;
    }

    @Nonnull
    public ResourceID getResourceManagerResourceId() {
        return resourceManagerResourceId;
    }

    @Nonnull
    public InstanceID getTaskExecutorRegistrationId() {
        return taskExecutorRegistrationId;
    }
}
