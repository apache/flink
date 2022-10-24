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
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a TaskExecutor.
 */
public final class TaskExecutorRegistrationSuccess extends RegistrationResponse.Success
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final InstanceID registrationId;

    private final ResourceID resourceManagerResourceId;

    private final ClusterInformation clusterInformation;

    @Nullable private final byte[] initialTokens;

    /**
     * Create a new {@code TaskExecutorRegistrationSuccess} message.
     *
     * @param registrationId The ID that the ResourceManager assigned the registration.
     * @param resourceManagerResourceId The unique ID that identifies the ResourceManager.
     * @param clusterInformation information about the cluster
     * @param initialTokens initial tokens for the TaskExecutor
     */
    public TaskExecutorRegistrationSuccess(
            InstanceID registrationId,
            ResourceID resourceManagerResourceId,
            ClusterInformation clusterInformation,
            @Nullable byte[] initialTokens) {
        this.registrationId = Preconditions.checkNotNull(registrationId);
        this.resourceManagerResourceId = Preconditions.checkNotNull(resourceManagerResourceId);
        this.clusterInformation = Preconditions.checkNotNull(clusterInformation);
        this.initialTokens = initialTokens;
    }

    /** Gets the ID that the ResourceManager assigned the registration. */
    public InstanceID getRegistrationId() {
        return registrationId;
    }

    /** Gets the unique ID that identifies the ResourceManager. */
    public ResourceID getResourceManagerId() {
        return resourceManagerResourceId;
    }

    /** Gets the cluster information. */
    public ClusterInformation getClusterInformation() {
        return clusterInformation;
    }

    /** Gets the initial tokens. */
    public byte[] getInitialTokens() {
        return initialTokens;
    }

    @Override
    public String toString() {
        return "TaskExecutorRegistrationSuccess{"
                + "registrationId="
                + registrationId
                + ", resourceManagerResourceId="
                + resourceManagerResourceId
                + ", clusterInformation="
                + clusterInformation
                + '}';
    }
}
