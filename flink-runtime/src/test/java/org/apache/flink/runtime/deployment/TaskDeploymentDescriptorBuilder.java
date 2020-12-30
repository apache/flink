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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Builder for {@link TaskDeploymentDescriptor}. */
public class TaskDeploymentDescriptorBuilder {
    private JobID jobId;
    private MaybeOffloaded<JobInformation> serializedJobInformation;
    private MaybeOffloaded<TaskInformation> serializedTaskInformation;
    private ExecutionAttemptID executionId;
    private AllocationID allocationId;
    private int subtaskIndex;
    private int attemptNumber;
    private List<ResultPartitionDeploymentDescriptor> producedPartitions;
    private List<InputGateDeploymentDescriptor> inputGates;
    private int targetSlotNumber;

    @Nullable private JobManagerTaskRestore taskRestore;

    private TaskDeploymentDescriptorBuilder(JobID jobId, String invokableClassName)
            throws IOException {
        TaskInformation taskInformation =
                new TaskInformation(
                        new JobVertexID(),
                        "test task",
                        1,
                        1,
                        invokableClassName,
                        new Configuration());

        this.jobId = jobId;
        this.serializedJobInformation =
                new NonOffloaded<>(
                        new SerializedValue<>(new DummyJobInformation(jobId, "DummyJob")));
        this.serializedTaskInformation = new NonOffloaded<>(new SerializedValue<>(taskInformation));
        this.executionId = new ExecutionAttemptID();
        this.allocationId = new AllocationID();
        this.subtaskIndex = 0;
        this.attemptNumber = 0;
        this.producedPartitions = Collections.emptyList();
        this.inputGates = Collections.emptyList();
        this.targetSlotNumber = 0;
        this.taskRestore = null;
    }

    public TaskDeploymentDescriptorBuilder setSerializedJobInformation(
            MaybeOffloaded<JobInformation> serializedJobInformation) {
        this.serializedJobInformation = serializedJobInformation;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setSerializedTaskInformation(
            MaybeOffloaded<TaskInformation> serializedTaskInformation) {
        this.serializedTaskInformation = serializedTaskInformation;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setJobId(JobID jobId) {
        this.jobId = jobId;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setExecutionId(ExecutionAttemptID executionId) {
        this.executionId = executionId;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setAllocationId(AllocationID allocationId) {
        this.allocationId = allocationId;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setSubtaskIndex(int subtaskIndex) {
        this.subtaskIndex = subtaskIndex;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setProducedPartitions(
            List<ResultPartitionDeploymentDescriptor> producedPartitions) {
        this.producedPartitions = producedPartitions;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setInputGates(
            List<InputGateDeploymentDescriptor> inputGates) {
        this.inputGates = inputGates;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setTargetSlotNumber(int targetSlotNumber) {
        this.targetSlotNumber = targetSlotNumber;
        return this;
    }

    public TaskDeploymentDescriptorBuilder setTaskRestore(
            @Nullable JobManagerTaskRestore taskRestore) {
        this.taskRestore = taskRestore;
        return this;
    }

    public TaskDeploymentDescriptor build() {
        return new TaskDeploymentDescriptor(
                jobId,
                serializedJobInformation,
                serializedTaskInformation,
                executionId,
                allocationId,
                subtaskIndex,
                attemptNumber,
                targetSlotNumber,
                taskRestore,
                producedPartitions,
                inputGates);
    }

    public static TaskDeploymentDescriptorBuilder newBuilder(JobID jobId, Class<?> invokableClass)
            throws IOException {
        return new TaskDeploymentDescriptorBuilder(jobId, invokableClass.getName());
    }
}
