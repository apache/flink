/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link TaskExecutorToResourceManagerConnection}. */
public class TaskExecutorToResourceManagerConnectionTest extends TestLogger {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(TaskExecutorToResourceManagerConnectionTest.class);

    private static final int TEST_TIMEOUT_MILLIS = 10000;

    private static final String RESOURCE_MANAGER_ADDRESS = "localhost";

    private static final ResourceManagerId RESOURCE_MANAGER_ID = ResourceManagerId.generate();

    private static final String TASK_MANAGER_ADDRESS = "localhost";

    private static final ResourceID TASK_MANAGER_RESOURCE_ID = ResourceID.generate();

    private static final int TASK_MANAGER_DATA_PORT = 12345;

    private static final int TASK_MANAGER_JMX_PORT = 23456;

    private static final HardwareDescription TASK_MANAGER_HARDWARE_DESCRIPTION =
            HardwareDescription.extractFromSystem(Long.MAX_VALUE);

    private static final TaskExecutorMemoryConfiguration TASK_MANAGER_MEMORY_CONFIGURATION =
            new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);

    private TestingRpcService rpcService;

    private TestingResourceManagerGateway testingResourceManagerGateway;

    private CompletableFuture<Void> registrationSuccessFuture;

    private CompletableFuture<Void> registrationRejectionFuture;

    @Test
    public void testResourceManagerRegistration() throws Exception {
        final TaskExecutorToResourceManagerConnection resourceManagerRegistration =
                createTaskExecutorToResourceManagerConnection();

        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> {
                    final String actualAddress = taskExecutorRegistration.getTaskExecutorAddress();
                    final ResourceID actualResourceId = taskExecutorRegistration.getResourceId();
                    final Integer actualDataPort = taskExecutorRegistration.getDataPort();
                    final HardwareDescription actualHardwareDescription =
                            taskExecutorRegistration.getHardwareDescription();
                    final TaskExecutorMemoryConfiguration actualMemoryConfiguration =
                            taskExecutorRegistration.getMemoryConfiguration();

                    assertThat(actualAddress, is(equalTo(TASK_MANAGER_ADDRESS)));
                    assertThat(actualResourceId, is(equalTo(TASK_MANAGER_RESOURCE_ID)));
                    assertThat(actualDataPort, is(equalTo(TASK_MANAGER_DATA_PORT)));
                    assertThat(
                            actualHardwareDescription,
                            is(equalTo(TASK_MANAGER_HARDWARE_DESCRIPTION)));
                    assertThat(actualMemoryConfiguration, is(TASK_MANAGER_MEMORY_CONFIGURATION));

                    return CompletableFuture.completedFuture(successfulRegistration());
                });

        resourceManagerRegistration.start();
        registrationSuccessFuture.get(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testResourceManagerRegistrationIsRejected() {
        final TaskExecutorToResourceManagerConnection resourceManagerRegistration =
                createTaskExecutorToResourceManagerConnection();

        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                taskExecutorRegistration -> {
                    return CompletableFuture.completedFuture(
                            new TaskExecutorRegistrationRejection("Foobar"));
                });

        resourceManagerRegistration.start();
        registrationRejectionFuture.join();
    }

    private TaskExecutorToResourceManagerConnection
            createTaskExecutorToResourceManagerConnection() {
        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        TASK_MANAGER_ADDRESS,
                        TASK_MANAGER_RESOURCE_ID,
                        TASK_MANAGER_DATA_PORT,
                        TASK_MANAGER_JMX_PORT,
                        TASK_MANAGER_HARDWARE_DESCRIPTION,
                        TASK_MANAGER_MEMORY_CONFIGURATION,
                        ResourceProfile.ZERO,
                        ResourceProfile.ZERO);
        return new TaskExecutorToResourceManagerConnection(
                LOGGER,
                rpcService,
                RetryingRegistrationConfiguration.defaultConfiguration(),
                RESOURCE_MANAGER_ADDRESS,
                RESOURCE_MANAGER_ID,
                Executors.directExecutor(),
                new TestRegistrationConnectionListener<>(),
                taskExecutorRegistration);
    }

    private static TaskExecutorRegistrationSuccess successfulRegistration() {
        return new TaskExecutorRegistrationSuccess(
                new InstanceID(),
                ResourceID.generate(),
                new ClusterInformation("blobServerHost", 55555));
    }

    @Before
    public void setUp() {
        rpcService = new TestingRpcService();

        testingResourceManagerGateway = new TestingResourceManagerGateway();
        rpcService.registerGateway(RESOURCE_MANAGER_ADDRESS, testingResourceManagerGateway);

        registrationSuccessFuture = new CompletableFuture<>();
        registrationRejectionFuture = new CompletableFuture<>();
    }

    @After
    public void tearDown() throws Exception {
        rpcService.stopService().get(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    private class TestRegistrationConnectionListener<
                    T extends RegisteredRpcConnection<?, ?, S, ?>,
                    S extends RegistrationResponse.Success,
                    R extends RegistrationResponse.Rejection>
            implements RegistrationConnectionListener<T, S, R> {

        @Override
        public void onRegistrationSuccess(final T connection, final S success) {
            registrationSuccessFuture.complete(null);
        }

        @Override
        public void onRegistrationFailure(final Throwable failure) {
            registrationSuccessFuture.completeExceptionally(failure);
        }

        @Override
        public void onRegistrationRejection(String targetAddress, R rejection) {
            registrationRejectionFuture.complete(null);
        }
    }
}
