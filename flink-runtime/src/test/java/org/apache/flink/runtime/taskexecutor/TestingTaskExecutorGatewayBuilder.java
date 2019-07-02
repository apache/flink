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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builder for a {@link TestingTaskExecutorGateway}.
 */
public class TestingTaskExecutorGatewayBuilder {

	private static final BiConsumer<ResourceID, AllocatedSlotReport> NOOP_HEARTBEAT_JOBMANAGER_CONSUMER = (ignoredA, ignoredB) -> {};
	private static final BiConsumer<JobID, Throwable> NOOP_DISCONNECT_JOBMANAGER_CONSUMER = (ignoredA, ignoredB) -> {};
	private static final BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> NOOP_SUBMIT_TASK_CONSUMER = (ignoredA, ignoredB) -> CompletableFuture.completedFuture(Acknowledge.get());
	private static final Function<Tuple5<SlotID, JobID, AllocationID, String, ResourceManagerId>, CompletableFuture<Acknowledge>> NOOP_REQUEST_SLOT_FUNCTION = ignored -> CompletableFuture.completedFuture(Acknowledge.get());
	private static final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> NOOP_FREE_SLOT_FUNCTION = (ignoredA, ignoredB) -> CompletableFuture.completedFuture(Acknowledge.get());
	private static final Consumer<ResourceID> NOOP_HEARTBEAT_RESOURCE_MANAGER_CONSUMER = ignored -> {};
	private static final Consumer<Exception> NOOP_DISCONNECT_RESOURCE_MANAGER_CONSUMER = ignored -> {};
	private static final Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> NOOP_CANCEL_TASK_FUNCTION = ignored -> CompletableFuture.completedFuture(Acknowledge.get());
	private static final BiConsumer<JobID, Collection<ResultPartitionID>> NOOP_RELEASE_PARTITIONS_CONSUMER = (ignoredA, ignoredB) -> {};

	private String address = "foobar:1234";
	private String hostname = "foobar";
	private BiConsumer<ResourceID, AllocatedSlotReport> heartbeatJobManagerConsumer = NOOP_HEARTBEAT_JOBMANAGER_CONSUMER;
	private BiConsumer<JobID, Throwable> disconnectJobManagerConsumer = NOOP_DISCONNECT_JOBMANAGER_CONSUMER;
	private BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer = NOOP_SUBMIT_TASK_CONSUMER;
	private Function<Tuple5<SlotID, JobID, AllocationID, String, ResourceManagerId>, CompletableFuture<Acknowledge>> requestSlotFunction = NOOP_REQUEST_SLOT_FUNCTION;
	private BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction = NOOP_FREE_SLOT_FUNCTION;
	private Consumer<ResourceID> heartbeatResourceManagerConsumer = NOOP_HEARTBEAT_RESOURCE_MANAGER_CONSUMER;
	private Consumer<Exception> disconnectResourceManagerConsumer = NOOP_DISCONNECT_RESOURCE_MANAGER_CONSUMER;
	private Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction = NOOP_CANCEL_TASK_FUNCTION;
	private Supplier<Boolean> canBeReleasedSupplier = () -> true;
	private BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer = NOOP_RELEASE_PARTITIONS_CONSUMER;

	public TestingTaskExecutorGatewayBuilder setAddress(String address) {
		this.address = address;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setHostname(String hostname) {
		this.hostname = hostname;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setHeartbeatJobManagerConsumer(BiConsumer<ResourceID, AllocatedSlotReport> heartbeatJobManagerConsumer) {
		this.heartbeatJobManagerConsumer = heartbeatJobManagerConsumer;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setDisconnectJobManagerConsumer(BiConsumer<JobID, Throwable> disconnectJobManagerConsumer) {
		this.disconnectJobManagerConsumer = disconnectJobManagerConsumer;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setSubmitTaskConsumer(BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer) {
		this.submitTaskConsumer = submitTaskConsumer;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setRequestSlotFunction(Function<Tuple5<SlotID, JobID, AllocationID, String, ResourceManagerId>, CompletableFuture<Acknowledge>> requestSlotFunction) {
		this.requestSlotFunction = requestSlotFunction;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setFreeSlotFunction(BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction) {
		this.freeSlotFunction = freeSlotFunction;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setHeartbeatResourceManagerConsumer(Consumer<ResourceID> heartbeatResourceManagerConsumer) {
		this.heartbeatResourceManagerConsumer = heartbeatResourceManagerConsumer;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setDisconnectResourceManagerConsumer(Consumer<Exception> disconnectResourceManagerConsumer) {
		this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setCancelTaskFunction(Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction) {
		this.cancelTaskFunction = cancelTaskFunction;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setCanBeReleasedSupplier(Supplier<Boolean> canBeReleasedSupplier) {
		this.canBeReleasedSupplier = canBeReleasedSupplier;
		return this;
	}

	public TestingTaskExecutorGatewayBuilder setReleasePartitionsConsumer(BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer) {
		this.releasePartitionsConsumer = releasePartitionsConsumer;
		return this;
	}

	public TestingTaskExecutorGateway createTestingTaskExecutorGateway() {
		return new TestingTaskExecutorGateway(
			address,
			hostname,
			heartbeatJobManagerConsumer,
			disconnectJobManagerConsumer,
			submitTaskConsumer,
			requestSlotFunction,
			freeSlotFunction,
			heartbeatResourceManagerConsumer,
			disconnectResourceManagerConsumer,
			cancelTaskFunction,
			canBeReleasedSupplier,
			releasePartitionsConsumer);
	}
}
