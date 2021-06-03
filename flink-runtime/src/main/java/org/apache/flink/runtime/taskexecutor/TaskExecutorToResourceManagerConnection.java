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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The connection between a TaskExecutor and the ResourceManager. */
public class TaskExecutorToResourceManagerConnection
        extends RegisteredRpcConnection<
                ResourceManagerId,
                ResourceManagerGateway,
                TaskExecutorRegistrationSuccess,
                TaskExecutorRegistrationRejection> {

    private final RpcService rpcService;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private final RegistrationConnectionListener<
                    TaskExecutorToResourceManagerConnection,
                    TaskExecutorRegistrationSuccess,
                    TaskExecutorRegistrationRejection>
            registrationListener;

    private final TaskExecutorRegistration taskExecutorRegistration;

    public TaskExecutorToResourceManagerConnection(
            Logger log,
            RpcService rpcService,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration,
            String resourceManagerAddress,
            ResourceManagerId resourceManagerId,
            Executor executor,
            RegistrationConnectionListener<
                            TaskExecutorToResourceManagerConnection,
                            TaskExecutorRegistrationSuccess,
                            TaskExecutorRegistrationRejection>
                    registrationListener,
            TaskExecutorRegistration taskExecutorRegistration) {

        super(log, resourceManagerAddress, resourceManagerId, executor);

        this.rpcService = checkNotNull(rpcService);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);
        this.registrationListener = checkNotNull(registrationListener);
        this.taskExecutorRegistration = checkNotNull(taskExecutorRegistration);
    }

    @Override
    protected RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    TaskExecutorRegistrationSuccess,
                    TaskExecutorRegistrationRejection>
            generateRegistration() {
        return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
                log,
                rpcService,
                getTargetAddress(),
                getTargetLeaderId(),
                retryingRegistrationConfiguration,
                taskExecutorRegistration);
    }

    @Override
    protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
        log.info(
                "Successful registration at resource manager {} under registration id {}.",
                getTargetAddress(),
                success.getRegistrationId());

        registrationListener.onRegistrationSuccess(this, success);
    }

    @Override
    protected void onRegistrationRejection(TaskExecutorRegistrationRejection rejection) {
        registrationListener.onRegistrationRejection(getTargetAddress(), rejection);
    }

    @Override
    protected void onRegistrationFailure(Throwable failure) {
        log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

        registrationListener.onRegistrationFailure(failure);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static class ResourceManagerRegistration
            extends RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    TaskExecutorRegistrationSuccess,
                    TaskExecutorRegistrationRejection> {

        private final TaskExecutorRegistration taskExecutorRegistration;

        ResourceManagerRegistration(
                Logger log,
                RpcService rpcService,
                String targetAddress,
                ResourceManagerId resourceManagerId,
                RetryingRegistrationConfiguration retryingRegistrationConfiguration,
                TaskExecutorRegistration taskExecutorRegistration) {

            super(
                    log,
                    rpcService,
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    targetAddress,
                    resourceManagerId,
                    retryingRegistrationConfiguration);
            this.taskExecutorRegistration = taskExecutorRegistration;
        }

        @Override
        protected CompletableFuture<RegistrationResponse> invokeRegistration(
                ResourceManagerGateway resourceManager,
                ResourceManagerId fencingToken,
                long timeoutMillis)
                throws Exception {

            Time timeout = Time.milliseconds(timeoutMillis);
            return resourceManager.registerTaskExecutor(taskExecutorRegistration, timeout);
        }
    }
}
