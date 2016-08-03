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

package org.apache.flink.runtime.rpc.jobmaster;

import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.resourcemanager.JobMasterRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;

import java.util.concurrent.ExecutorService;

public class JobMaster implements RpcServer<JobMasterGateway> {
	private final Logger LOG = LoggerFactory.getLogger(JobMaster.class);
	private final RpcService rpcService;
	private final ExecutionContext executionContext;
	private JobMasterGateway self;

	private ResourceManagerGateway resourceManager = null;

	public JobMaster(RpcService rpcService, ExecutorService executorService) {
		this.rpcService = rpcService;
		executionContext = ExecutionContext$.MODULE$.fromExecutor(executorService);
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	@Override
	public void start() {
		// start rpc server
		self = rpcService.startServer(this, JobMasterGateway.class);
	}

	@Override
	public void shutDown() {
		rpcService.stopServer(getSelf());
	}

	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	@RpcMethod
	public void triggerResourceManagerRegistration(final String address) {
		Future<ResourceManagerGateway> resourceManagerFuture = rpcService.connect(address, ResourceManagerGateway.class);

		Future<RegistrationResponse> registrationResponseFuture = resourceManagerFuture.flatMap(new Mapper<ResourceManagerGateway, Future<RegistrationResponse>>() {
			@Override
			public Future<RegistrationResponse> apply(ResourceManagerGateway resourceManagerGateway) {
 				return resourceManagerGateway.registerJobMaster(new JobMasterRegistration());
			}
		}, executionContext);

		resourceManagerFuture.zip(registrationResponseFuture).onComplete(new OnComplete<Tuple2<ResourceManagerGateway, RegistrationResponse>>() {
			@Override
			public void onComplete(Throwable failure, Tuple2<ResourceManagerGateway, RegistrationResponse> success) throws Throwable {
				if (failure != null) {
					LOG.info("Registration at resource manager {} failed. Tyr again.", address);
				} else {
					getSelf().handleRegistrationResponse(success._2(), success._1());
				}
			}
		}, executionContext);
	}

	@RpcMethod
	public void handleRegistrationResponse(RegistrationResponse response, ResourceManagerGateway resourceManager) {
		System.out.println("Received registration response: " + response);
		this.resourceManager = resourceManager;
	}

	public boolean isConnected() {
		return resourceManager != null;
	}

	public JobMasterGateway getSelf() {
		return self;
	}
}
