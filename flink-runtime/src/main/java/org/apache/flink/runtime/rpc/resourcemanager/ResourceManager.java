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
import akka.dispatch.Recover;
import akka.util.Timeout;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ResourceManager extends RpcServer<ResourceManagerGateway> {
	private final ExecutionContext executionContext;
	private final Map<JobMasterGateway, InstanceID> jobMasterGateways;
	private final Timeout callableTimeout = new Timeout(10, TimeUnit.SECONDS);

	public ResourceManager(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(executorService);
		this.jobMasterGateways = new HashMap<>();
	}

	@RpcMethod
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		Future<JobMasterGateway> jobMasterFuture = getRpcService().connect(jobMasterRegistration.getAddress(), JobMasterGateway.class);

		return jobMasterFuture.flatMap(new Mapper<JobMasterGateway, Future<RegistrationResponse>>() {
			@Override
			public Future<RegistrationResponse> apply(final JobMasterGateway jobMasterGateway) {
				Future<InstanceID> instanceIDFuture = callAsync(new Callable<InstanceID> () {
					@Override
					public InstanceID call() throws Exception {
						if (jobMasterGateways.containsKey(jobMasterGateway)) {
							return jobMasterGateways.get(jobMasterGateway);
						} else {
							InstanceID instanceID = new InstanceID();
							jobMasterGateways.put(jobMasterGateway, instanceID);

							return instanceID;
						}
					}
				}, callableTimeout);

				return instanceIDFuture.map(new Mapper<InstanceID, RegistrationResponse>() {
					@Override
					public RegistrationResponse apply(InstanceID parameter) {
						return new RegistrationResponse(true, parameter);
					}
				}, executionContext).recover(new Recover<RegistrationResponse>() {
					@Override
					public RegistrationResponse recover(Throwable failure) throws Throwable {
						return new RegistrationResponse(false, null);
					}
				}, executionContext);
			}
		}, executionContext);
	}

	@RpcMethod
	public SlotAssignment requestSlot(SlotRequest slotRequest) {
		System.out.println("SlotRequest: " + slotRequest);
		return new SlotAssignment();
	}
}
