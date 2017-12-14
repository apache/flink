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

package org.apache.flink.runtime.resourcemanager.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Implementation of the {@link ResourceManagerGateway} for testing purposes solely.
 */
public class TestingResourceManagerGateway implements ResourceManagerGateway {

	private final ResourceManagerId resourceManagerId;

	private final ResourceID resourceId;

	private final long heartbeatInterval;

	private final String address;

	private final String hostname;

	private final AtomicReference<CompletableFuture<Acknowledge>> slotFutureReference;

	private volatile Consumer<AllocationID> cancelSlotConsumer;

	private volatile Consumer<SlotRequest> requestSlotConsumer;

	public TestingResourceManagerGateway() {
		this(
			ResourceManagerId.generate(),
			ResourceID.generate(),
			10000L,
			"localhost",
			"localhost");
	}

	public TestingResourceManagerGateway(
			ResourceManagerId resourceManagerId,
			ResourceID resourceId,
			long heartbeatInterval,
			String address,
			String hostname) {
		this.resourceManagerId = Preconditions.checkNotNull(resourceManagerId);
		this.resourceId = Preconditions.checkNotNull(resourceId);
		this.heartbeatInterval = heartbeatInterval;
		this.address = Preconditions.checkNotNull(address);
		this.hostname = Preconditions.checkNotNull(hostname);
		this.slotFutureReference = new AtomicReference<>();
		this.cancelSlotConsumer = null;
		this.requestSlotConsumer = null;
	}

	public void setRequestSlotFuture(CompletableFuture<Acknowledge> slotFuture) {
		this.slotFutureReference.set(slotFuture);
	}

	public void setCancelSlotConsumer(Consumer<AllocationID> cancelSlotConsumer) {
		this.cancelSlotConsumer = cancelSlotConsumer;
	}

	public void setRequestSlotConsumer(Consumer<SlotRequest> slotRequestConsumer) {
		this.requestSlotConsumer = slotRequestConsumer;
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(JobMasterId jobMasterId, ResourceID jobMasterResourceId, String jobMasterAddress, JobID jobId, Time timeout) {
		return CompletableFuture.completedFuture(
			new JobMasterRegistrationSuccess(
				heartbeatInterval,
				resourceManagerId,
				resourceId));
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(JobMasterId jobMasterId, SlotRequest slotRequest, Time timeout) {
		Consumer<SlotRequest> currentRequestSlotConsumer = requestSlotConsumer;

		if (currentRequestSlotConsumer != null) {
			currentRequestSlotConsumer.accept(slotRequest);
		}

		CompletableFuture<Acknowledge> slotFuture = slotFutureReference.getAndSet(null);

		if (slotFuture != null) {
			return slotFuture;
		} else {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public void cancelSlotRequest(AllocationID allocationID) {
		Consumer<AllocationID> currentCancelSlotConsumer = cancelSlotConsumer;

		if (currentCancelSlotConsumer != null) {
			currentCancelSlotConsumer.accept(allocationID);
		}
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(String taskExecutorAddress, ResourceID resourceId, SlotReport slotReport, int dataPort, HardwareDescription hardwareDescription, Time timeout) {
		return CompletableFuture.completedFuture(
			new TaskExecutorRegistrationSuccess(
				new InstanceID(),
				resourceId,
				heartbeatInterval));
	}

	@Override
	public void notifySlotAvailable(InstanceID instanceId, SlotID slotID, AllocationID oldAllocationId) {

	}

	@Override
	public void registerInfoMessageListener(String infoMessageListenerAddress) {

	}

	@Override
	public void unRegisterInfoMessageListener(String infoMessageListenerAddress) {

	}

	@Override
	public void shutDownCluster(ApplicationStatus finalStatus, String optionalDiagnostics) {

	}

	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return CompletableFuture.completedFuture(0);
	}

	@Override
	public void heartbeatFromTaskManager(ResourceID heartbeatOrigin, SlotReport slotReport) {

	}

	@Override
	public void heartbeatFromJobManager(ResourceID heartbeatOrigin) {

	}

	@Override
	public void disconnectTaskManager(ResourceID resourceID, Exception cause) {

	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {

	}

	@Override
	public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {
		return CompletableFuture.completedFuture(Collections.emptyList());
	}

	@Override
	public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID resourceId, Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException("Not yet implemented"));
	}

	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException("Not yet implemented"));
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		return CompletableFuture.completedFuture(Collections.emptyList());
	}

	@Override
	public ResourceManagerId getFencingToken() {
		return resourceManagerId;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public String getHostname() {
		return hostname;
	}
}
