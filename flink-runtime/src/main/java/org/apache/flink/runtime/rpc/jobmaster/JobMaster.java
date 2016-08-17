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

import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.resourcemanager.JobMasterRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;
import scala.Tuple2;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.*;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link org.apache.flink.runtime.jobgraph.JobGraph}.
 *
 * It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 *     <li>{@link #registerAtResourceManager(String)} triggers the registration at the resource manager</li>
 *     <li>{@link #updateTaskExecutionState(TaskExecutionState)} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends RpcEndpoint<JobMasterGateway> implements LeaderContender {
	/** Execution context for future callbacks */
	private final ExecutionContext executionContext;

	/** Execution context for scheduled runnables */
	private final ScheduledExecutorService scheduledExecutorService;

	private final FiniteDuration initialRegistrationTimeout = new FiniteDuration(500, TimeUnit.MILLISECONDS);
	private final FiniteDuration maxRegistrationTimeout = new FiniteDuration(30, TimeUnit.SECONDS);
	private final FiniteDuration registrationDuration = new FiniteDuration(365, TimeUnit.DAYS);
	private final long failedRegistrationDelay = 10000;

	/** Gateway to connected resource manager, null iff not connected */
	private ResourceManagerGateway resourceManager = null;

	/** UUID to filter out old registration runs */
	private UUID currentRegistrationRun;

	/** Configuration of the job */
	private Configuration configuration;
	private RecoveryMode recoveryMode;

	/** Leader Management */
	private LeaderElectionService leaderElectionService = null;
	private UUID leaderSessionID;

	public JobMaster(Configuration configuration, RpcService rpcService, ExecutorService executorService) {
		super(rpcService);

		this.configuration = configuration;
		this.executionContext = ExecutionContext$.MODULE$.fromExecutor(
			Preconditions.checkNotNull(executorService));
		this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

		// Create necessary components
		createRecoveryComponents();

		// Start the leadership election service
		try {
			this.leaderElectionService.start(this);
		} catch (Exception e) {
			throw new RuntimeException("Fail to start the leader election service.", e);
		}
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	//----------------------------------------------------------------------------------------------
	// Initialization methods
	//----------------------------------------------------------------------------------------------

	/*
	 * Create recovery related components, including:
	 * (1) LeaderSelectionService: leadership management among JobMasters
	 * ...
	 */
	public void createRecoveryComponents() {
		recoveryMode = RecoveryMode.fromConfig(configuration);

		switch (recoveryMode) {
			case STANDALONE:
				leaderElectionService = new StandaloneLeaderElectionService();
				break;
			case ZOOKEEPER:
				try {
					CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
					leaderElectionService = ZooKeeperUtils.createLeaderElectionService(client, configuration);
				} catch (Exception e) {
					throw new RuntimeException("Fail to create zookeeper leader election service.", e);
				}
				break;
			default:
				throw new RuntimeException("Unknown recovery mode.");
		}
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Stop the execution when the leadership is revoked.
	 */
	public void revokeLeadership() {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("JobManager {} was revoked leadership.", getAddress());

				// TODO:: cancel the job's execution and notify all listeners
				cancelAndClearEverything(new Exception("JobManager is no longer the leader."));

				leaderSessionID = null;
			}
		});
	}

	/**
	 * Start the execution when the leadership is granted.
	 *
	 * @param newLeaderSessionID The identifier of the new leadership session
	 */
	public void grantLeadership(final UUID newLeaderSessionID) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.info("JobManager {} grants leadership with session id {}.", getAddress(), newLeaderSessionID);

				// The operation may be blocking, but since JM is idle before it grants the leadership, it's okay that
				// JM waits here for the operation's completeness.
				leaderSessionID = newLeaderSessionID;
				leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);

				// TODO:: register at the RM to start the scheduling
			}
		});
	}

	/**
	 * Handles error occurring in the leader election service
	 *
	 * @param exception Exception thrown in the leader election service
	 */
	public void handleError(final Exception exception) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.error("Received an error from the LeaderElectionService.", exception);

				// TODO:: cancel the job's execution and shutdown the JM
				cancelAndClearEverything(exception);

				leaderSessionID = null;
			}
		});

	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	/**
	 * Triggers the registration of the job master at the resource manager.
	 *
	 * @param address Address of the resource manager
	 */
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

	//----------------------------------------------------------------------------------------------
	// Helper methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Helper method to handle the resource manager registration process. If a registration attempt
	 * times out, then a new attempt with the doubled time out is initiated. The whole registration
	 * process has a deadline. Once this deadline is overdue without successful registration, the
	 * job master shuts down.
	 *
	 * @param jobMasterRegistration Job master registration info which is sent to the resource
	 *                              manager
	 * @param attemptNumber Registration attempt number
	 * @param resourceManagerFuture Future of the resource manager gateway
	 * @param registrationRun UUID describing the current registration run
	 * @param timeout Timeout of the last registration attempt
	 * @param maxTimeout Maximum timeout between registration attempts
	 * @param deadline Deadline for the registration
	 */
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

			log.info("Start registration attempt #{}.", attemptNumber);

			if (deadline.isOverdue()) {
				// we've exceeded our registration deadline. This means that we have to shutdown the JobMaster
				log.error("Exceeded registration deadline without successfully registering at the ResourceManager.");
				shutDown();
			} else {
				Future<Tuple2<RegistrationResponse, ResourceManagerGateway>> registrationResponseFuture = resourceManagerFuture.flatMap(new Mapper<ResourceManagerGateway, Future<Tuple2<RegistrationResponse, ResourceManagerGateway>>>() {
					@Override
					public Future<Tuple2<RegistrationResponse, ResourceManagerGateway>> apply(ResourceManagerGateway resourceManagerGateway) {
						return resourceManagerGateway.registerJobMaster(jobMasterRegistration, timeout).zip(Futures.successful(resourceManagerGateway));
					}
				}, executionContext);

				registrationResponseFuture.onComplete(new OnComplete<Tuple2<RegistrationResponse, ResourceManagerGateway>>() {
					@Override
					public void onComplete(Throwable failure, Tuple2<RegistrationResponse, ResourceManagerGateway> tuple) throws Throwable {
						if (failure != null) {
							if (failure instanceof TimeoutException) {
								// we haven't received an answer in the given timeout interval,
								// so increase it and try again.
								final FiniteDuration newTimeout = timeout.$times(2L).min(maxTimeout);

								handleResourceManagerRegistration(
									jobMasterRegistration,
									attemptNumber + 1,
									resourceManagerFuture,
									registrationRun,
									newTimeout,
									maxTimeout,
									deadline);
							} else {
								log.error("Received unknown error while registering at the ResourceManager.", failure);
								shutDown();
							}
						} else {
							final RegistrationResponse response = tuple._1();
							final ResourceManagerGateway gateway = tuple._2();

							if (response.isSuccess()) {
								finishResourceManagerRegistration(gateway, response.getInstanceID());
							} else {
								log.info("The registration was refused. Try again.");

								scheduledExecutorService.schedule(new Runnable() {
									@Override
									public void run() {
										// we have to execute scheduled runnable in the main thread
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
				}, getMainThreadExecutionContext()); // use the main thread execution context to execute the call back in the main thread
			}
		} else {
			log.info("Discard out-dated registration run.");
		}
	}

	/**
	 * Finish the resource manager registration by setting the new resource manager gateway.
	 *
	 * @param resourceManager New resource manager gateway
	 * @param instanceID Instance id assigned by the resource manager
	 */
	void finishResourceManagerRegistration(ResourceManagerGateway resourceManager, InstanceID instanceID) {
		log.info("Successfully registered at the ResourceManager under instance id {}.", instanceID);
		this.resourceManager = resourceManager;
	}

	/**
	 * Return if the job master is connected to a resource manager.
	 *
	 * @return true if the job master is connected to the resource manager
	 */
	public boolean isConnected() {
		return resourceManager != null;
	}


	/**
	 * Cancel the current job and notify all listeners the job's cancellation.
	 *
	 * @param cause Cause for the cancelling.
	 */
	private void cancelAndClearEverything(Throwable cause) {
		// currently, nothing to do here
	}
}
