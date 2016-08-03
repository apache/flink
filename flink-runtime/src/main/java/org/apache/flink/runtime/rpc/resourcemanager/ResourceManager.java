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

import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import java.util.concurrent.ExecutorService;

public class ResourceManager implements RpcServer<ResourceManagerGateway> {
	private final RpcService rpcService;
	private final ExecutionContext executionContext;

	private ResourceManagerGateway self;

	public ResourceManager(RpcService rpcService, ExecutorService executorService) {
		this.rpcService = rpcService;
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(executorService);
	}

	public void start() {
		self = rpcService.startServer(this, ResourceManagerGateway.class);
	}

	public void shutDown() {
		rpcService.stopServer(getSelf());
	}

	@RpcMethod
	public RegistrationResponse registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		System.out.println("JobMasterRegistration: " + jobMasterRegistration);
		return new RegistrationResponse();
	}

	@RpcMethod
	public SlotAssignment requestSlot(SlotRequest slotRequest) {
		System.out.println("SlotRequest: " + slotRequest);
		return new SlotAssignment();
	}

	public ResourceManagerGateway getSelf() {
		return self;
	}
}
