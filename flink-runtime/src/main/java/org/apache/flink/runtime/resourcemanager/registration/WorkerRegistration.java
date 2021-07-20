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

import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.util.Preconditions;

/** This class extends the {@link TaskExecutorConnection}, adding the worker information. */
public class WorkerRegistration<WorkerType extends ResourceIDRetrievable>
        extends TaskExecutorConnection {

    private final WorkerType worker;

    private final int dataPort;

    private final int jmxPort;

    private final HardwareDescription hardwareDescription;

    private final TaskExecutorMemoryConfiguration memoryConfiguration;

    private final ResourceProfile totalResourceProfile;

    private final ResourceProfile defaultSlotResourceProfile;

    public WorkerRegistration(
            TaskExecutorGateway taskExecutorGateway,
            WorkerType worker,
            int dataPort,
            int jmxPort,
            HardwareDescription hardwareDescription,
            TaskExecutorMemoryConfiguration memoryConfiguration,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {

        super(worker.getResourceID(), taskExecutorGateway);

        this.worker = Preconditions.checkNotNull(worker);
        this.dataPort = dataPort;
        this.jmxPort = jmxPort;
        this.hardwareDescription = Preconditions.checkNotNull(hardwareDescription);
        this.memoryConfiguration = Preconditions.checkNotNull(memoryConfiguration);
        this.totalResourceProfile = Preconditions.checkNotNull(totalResourceProfile);
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
    }

    public WorkerType getWorker() {
        return worker;
    }

    public int getDataPort() {
        return dataPort;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public HardwareDescription getHardwareDescription() {
        return hardwareDescription;
    }

    public TaskExecutorMemoryConfiguration getMemoryConfiguration() {
        return memoryConfiguration;
    }

    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    public ResourceProfile getTotalResourceProfile() {
        return totalResourceProfile;
    }
}
