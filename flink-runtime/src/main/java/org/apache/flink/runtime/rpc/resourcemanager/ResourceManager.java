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

package org.apache.flink.runtime.rpc.resourcemanager;

import akka.dispatch.Mapper;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.rpc.taskexecutor.RequestSlotResponse;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 * <p>
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 * <li>{@link #registerJobMaster(JobMasterRegistration)} registers a {@link JobMaster} at the resource manager</li>
 * <li>{@link #requestSlot(SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public class ResourceManager extends RpcEndpoint<ResourceManagerGateway> {
	private final ExecutionContext executionContext;
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;
	private final Map<ResourceID, TaskExecutorGateway> taskExecutorGateways;
	private final SlotManager slotManager;

	public ResourceManager(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(
			Preconditions.checkNotNull(executorService));
		this.jobMasterGateways = new HashMap<>();
		this.taskExecutorGateways = new HashMap<>();
		// TODO
		this.slotManager = new StandaloneSlotManager(this);
	}

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @return Future registration response
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		Future<JobMasterGateway> jobMasterFuture = getRpcService().connect(
			jobMasterRegistration.getAddress(),
			JobMasterGateway.class);

		return jobMasterFuture.map(new Mapper<JobMasterGateway, RegistrationResponse>() {
			@Override
			public RegistrationResponse apply(final JobMasterGateway jobMasterGateway) {
				InstanceID instanceID;

				if (jobMasterGateways.containsKey(jobMasterGateway)) {
					instanceID = jobMasterGateways.get(jobMasterGateway);
				} else {
					instanceID = new InstanceID();
					jobMasterGateways.put(jobMasterGateway, instanceID);
				}

				return new RegistrationResponse(true, instanceID);
			}
		}, getMainThreadExecutionContext());
	}

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param slotRequest Slot request
	 * @return Slot assignment
	 */
	@RpcMethod
	public SlotAssignment requestSlot(SlotRequest slotRequest) {
		System.out.println("SlotRequest: " + slotRequest);
		return new SlotAssignment();
	}


	/**
	 * @param resourceManagerLeaderId The fencing token for the ResourceManager leader
	 * @param taskExecutorAddress     The address of the TaskExecutor that registers
	 * @param resourceID              The resource ID of the TaskExecutor that registers
	 * @param slotReport              The report describing available and allocated slots
	 * @return The response by the ResourceManager.
	 */
	@RpcMethod
	public Future<TaskExecutorRegistrationResponse> registerTaskExecutor(
		UUID resourceManagerLeaderId,
		String taskExecutorAddress,
		final ResourceID resourceID,
		SlotReport slotReport) {
		Future<TaskExecutorGateway> taskExecutorFuture =
			getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorFuture.map(new Mapper<TaskExecutorGateway, TaskExecutorRegistrationResponse>() {
			@Override
			public TaskExecutorRegistrationResponse apply(final TaskExecutorGateway taskExecutorGateway) {
				taskExecutorGateways.put(resourceID, taskExecutorGateway);
				return new TaskExecutorRegistrationResponse.Decline("");
			}
		}, getMainThreadExecutionContext());
	}

	/**
	 * Send slotRequest to TaskManager
	 * @param slotRequest slot request information
	 * @param slotID which slot is choosen
	 */
	@RpcMethod
	public void sendRequestSlotToTaskManager(final SlotRequest slotRequest, final SlotID slotID) {
		ResourceID resourceID = slotID.getResourceID();
		TaskExecutorGateway te = taskExecutorGateways.get(resourceID);
		if (te == null) {
			throw new RuntimeException("unknown taskManager, " + resourceID);
		} else {
			Future<RequestSlotResponse> response = te.requestSlotForJob(
				slotRequest.getAllocationID(),
				slotRequest.getJobID());
			response.onSuccess(new OnSuccess<RequestSlotResponse>() {
				@Override
				public void onSuccess(RequestSlotResponse result) throws Throwable {
					if (result instanceof RequestSlotResponse.Decline) {
						slotManager.declineSlotRequestFromTaskManager(slotRequest);
					}
				}
			}, getMainThreadExecutionContext());
			response.onFailure(new OnFailure() {
				@Override
				public void onFailure(Throwable failure) {
					slotManager.declineSlotRequestFromTaskManager(slotRequest);
				}
			}, getMainThreadExecutionContext());
		}
	}
}
