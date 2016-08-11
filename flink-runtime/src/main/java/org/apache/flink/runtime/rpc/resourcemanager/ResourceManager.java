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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.util.Preconditions;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * It offers the following methods as part of its rpc interface to interact with the him remotely:
 * <ul>
 *     <li>{@link #registerJobMaster(JobMasterRegistration)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(SlotRequest)} requests a slot from the resource manager</li>
 * </ul>
 */
public class ResourceManager extends RpcEndpoint<ResourceManagerGateway> {
	private final ExecutionContext executionContext;
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;

	public ResourceManager(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(
			Preconditions.checkNotNull(executorService));
		this.jobMasterGateways = new HashMap<>();
	}

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterRegistration Job master registration information
	 * @return Future registration response
	 */
	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		Future<JobMasterGateway> jobMasterFuture = getRpcService().connect(jobMasterRegistration.getAddress(), JobMasterGateway.class);

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

	@Override
	public Class<ResourceManagerGateway> getSelfGatewayType() {
		return ResourceManagerGateway.class;
	}
}
