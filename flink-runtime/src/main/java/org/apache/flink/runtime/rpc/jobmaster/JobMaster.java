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
import org.apache.flink.runtime.instance.InstanceID;
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
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JobMaster extends RpcServer<JobMasterGateway> {
	private final Logger LOG = LoggerFactory.getLogger(JobMaster.class);
	private final ExecutionContext executionContext;
	private final ScheduledExecutorService scheduledExecutorService;

	private final FiniteDuration initialRegistrationTimeout = new FiniteDuration(500, TimeUnit.MILLISECONDS);
	private final FiniteDuration maxRegistrationTimeout = new FiniteDuration(30, TimeUnit.SECONDS);
	private final FiniteDuration registrationDuration = new FiniteDuration(365, TimeUnit.DAYS);
	private final long failedRegistrationDelay = 10000;

	private ScheduledFuture<?> scheduledRegistration;

	private ResourceManagerGateway resourceManager = null;

	private UUID currentRegistrationRun;

	public JobMaster(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		executionContext = ExecutionContext$.MODULE$.fromExecutor(executorService);
		scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	@RpcMethod
	public void registerAtResourceManager(final String address) {
		currentRegistrationRun = UUID.randomUUID();

		Future<ResourceManagerGateway> resourceManagerFuture = getRpcService().connect(address, ResourceManagerGateway.class);

		handleResourceManagerRegistration(
			new JobMasterRegistration(getAddress()),
			1,
			resourceManagerFuture,
			currentRegistrationRun,
			initialRegistrationTimeout,
			maxRegistrationTimeout,
			registrationDuration.fromNow());
	}

	void handleResourceManagerRegistration(
		final JobMasterRegistration jobMasterRegistration,
		final int attemptNumber,
		final Future<ResourceManagerGateway> resourceManagerFuture,
		final UUID registrationRun,
		final FiniteDuration timeout,
		final FiniteDuration maxTimeout,
		final Deadline deadline) {

		// filter out concurrent registration runs
		if (registrationRun.equals(currentRegistrationRun)) {

			LOG.info("Start registration attempt #{}.", attemptNumber);

			if (deadline.isOverdue()) {
				// we've exceeded our registration deadline. This means that we have to shutdown the JobMaster
				LOG.error("Exceeded registration deadline without successfully registering at the ResourceManager.");
				runAsync(new Runnable() {
					@Override
					public void run() {
						shutDown();
					}
				});
			} else {
				Future<RegistrationResponse> registrationResponseFuture = resourceManagerFuture.flatMap(new Mapper<ResourceManagerGateway, Future<RegistrationResponse>>() {
					@Override
					public Future<RegistrationResponse> apply(ResourceManagerGateway resourceManagerGateway) {
						return resourceManagerGateway.registerJobMaster(jobMasterRegistration, timeout);
					}
				}, executionContext);

				registrationResponseFuture.zip(resourceManagerFuture).onComplete(new OnComplete<Tuple2<RegistrationResponse, ResourceManagerGateway>>() {
					@Override
					public void onComplete(Throwable failure, Tuple2<RegistrationResponse, ResourceManagerGateway> tuple) throws Throwable {
						if (failure != null) {
							if (failure instanceof TimeoutException) {
								// we haven't received an answer in the given timeout interval,
								// so increase it and try again.
								final FiniteDuration newTimeout = timeout.$times(2L).min(maxTimeout);

								// we have to execute handleResourceManagerRegistration in the main thread
								// because we need consistency wrt currentRegistrationRun
								runAsync(new Runnable() {
									@Override
									public void run() {
										handleResourceManagerRegistration(
											jobMasterRegistration,
											attemptNumber + 1,
											resourceManagerFuture,
											registrationRun,
											newTimeout,
											maxTimeout,
											deadline);
									}
								});
							} else {
								LOG.error("Received unknown error while registering at the ResourceManager.", failure);
								runAsync(new Runnable() {
									@Override
									public void run() {
										shutDown();
									}
								});
							}
						} else {
							final RegistrationResponse response = tuple._1();
							final ResourceManagerGateway gateway = tuple._2();

							if (response.isSuccess()) {
								runAsync(new Runnable() {
									@Override
									public void run() {
										finishResourceManagerRegistration(gateway, response.getInstanceID());
									}
								});
							} else {
								LOG.info("The registration was refused. Try again.");

								scheduledExecutorService.schedule(new Runnable() {
									@Override
									public void run() {
										// we have to execute handleResourceManagerRegistration in the main thread
										// because we need consistency wrt currentRegistrationRun
										runAsync(new Runnable() {
											@Override
											public void run() {
												// our registration attempt was refused. Start over.
												handleResourceManagerRegistration(
													jobMasterRegistration,
													1,
													resourceManagerFuture,
													registrationRun,
													initialRegistrationTimeout,
													maxTimeout,
													deadline);
											}
										});
									}
								}, failedRegistrationDelay, TimeUnit.MILLISECONDS);
							}
						}
					}
				}, executionContext);
			}
		} else {
			LOG.info("Discard out-dated registration run.");
		}
	}

	void finishResourceManagerRegistration(ResourceManagerGateway resourceManager, InstanceID instanceID) {
		LOG.info("Successfully registered at the ResourceManager under instance id {}.", instanceID);
		this.resourceManager = resourceManager;
	}

	public boolean isConnected() {
		return resourceManager != null;
	}
}
