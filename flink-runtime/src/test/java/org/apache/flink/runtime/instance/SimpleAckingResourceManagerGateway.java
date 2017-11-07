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

package org.apache.flink.runtime.instance;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A ResourceManagerGateway that simply acks the request slot operations and does not
 * support any more advanced operations.
 */
public class SimpleAckingResourceManagerGateway implements ResourceManagerGateway {

	@Override
	public String getAddress() {
		return null;
	}

	@Override
	public String getHostname() {
		return null;
	}

	@Override
	public ResourceManagerId getFencingToken() {
		return null;
	};

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(
			JobMasterId jobMasterId,
			ResourceID jobMasterResourceId,
			String jobMasterAddress,
			JobID jobId,
			@RpcTimeout Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(
			JobMasterId jobMasterId,
			SlotRequest slotRequest,
			@RpcTimeout Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(
			String taskExecutorAddress,
			ResourceID resourceId,
			SlotReport slotReport,
			@RpcTimeout Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public void notifySlotAvailable(
			InstanceID instanceId,
			SlotID slotID,
			AllocationID oldAllocationId) {}

	@Override
	public void registerInfoMessageListener(String infoMessageListenerAddress) {}

	@Override
	public void unRegisterInfoMessageListener(String infoMessageListenerAddress) {}

	@Override
	public void shutDownCluster(final ApplicationStatus finalStatus, final String optionalDiagnostics) {}

	@Override
	public void disconnectTaskManager(ResourceID resourceID, Exception cause) {}

	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID heartbeatOrigin, final SlotReport slotReport) {}

	@Override
	public void heartbeatFromJobManager(final ResourceID heartbeatOrigin) {}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {}

	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(@RpcTimeout Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(@RpcTimeout Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}
}
