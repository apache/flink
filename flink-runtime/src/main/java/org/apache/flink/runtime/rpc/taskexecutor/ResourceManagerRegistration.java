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

package org.apache.flink.runtime.rpc.taskexecutor;

import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;

import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.resourcemanager.TaskExecutorRegistrationResponse;

import org.slf4j.Logger;

import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * This utility class handles the registration of the TaskExecutor at the ResourceManager.
 * It implements the initial address resolution and the retries-with-backoff strategy.
 * 
 * <p>The registration process notifies its {@link TaskExecutorToResourceManagerConnection}
 * upon successful registration. The registration can be canceled, for example when the
 * ResourceManager that it tries to register at looses leader status.
 * 
 * <p>Implementation note: This class does not act like a single-threaded actor.
 * It holds only constant state and passes all variable state via stack and closures.
 */
public class ResourceManagerRegistration {

	// ------------------------------------------------------------------------
	//  configuration constants
	// ------------------------------------------------------------------------

	private static final long INITIAL_REGISTRATION_TIMEOUT_MILLIS = 100;

	private static final long MAX_REGISTRATION_TIMEOUT_MILLIS = 30000;

	private static final long ERROR_REGISTRATION_DELAY_MILLIS = 10000;

	private static final long REFUSED_REGISTRATION_DELAY_MILLIS = 30000;

	// ------------------------------------------------------------------------

	private final Logger log;

	private final TaskExecutorToResourceManagerConnection connection;

	private final TaskExecutor taskExecutor;

	private final String resourceManagerAddress;

	private final UUID resourceManagerLeaderId;

	private volatile boolean canceled;

	public ResourceManagerRegistration(
			Logger log,
			TaskExecutorToResourceManagerConnection connection,
			TaskExecutor taskExecutor,
			String resourceManagerAddress,
			UUID resourceManagerLeaderId) {

		this.log = checkNotNull(log);
		this.connection = checkNotNull(connection);
		this.taskExecutor = checkNotNull(taskExecutor);
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
		this.resourceManagerLeaderId = checkNotNull(resourceManagerLeaderId);
	}

	// ------------------------------------------------------------------------
	//  cancellation
	// ------------------------------------------------------------------------

	/**
	 * Cancels the registration procedure.
	 */
	public void cancel() {
		canceled = true;
	}

	/**
	 * Checks if the registration was canceled.
	 * @return True if the registration was canceled, false otherwise.
	 */
	public boolean isCanceled() {
		return canceled;
	}

	// ------------------------------------------------------------------------
	//  registration
	// ------------------------------------------------------------------------

	/**
	 * This method resolved the ResourceManager address to a callable gateway and starts the
	 * registration after that.
	 */
	@SuppressWarnings("unchecked")
	public void resolveResourceManagerAndStartRegistration() {
		final RpcService rpcService = taskExecutor.getRpcService();

		// trigger resolution of the resource manager address to a callable gateway
		Future<ResourceManagerGateway> resourceManagerFuture =
				rpcService.connect(resourceManagerAddress, ResourceManagerGateway.class);

		// upon success, start the registration attempts
		resourceManagerFuture.onSuccess(new OnSuccess<ResourceManagerGateway>() {
			@Override
			public void onSuccess(ResourceManagerGateway result) {
				log.info("Resolved ResourceManager address, beginning registration");
				register(result, 1, INITIAL_REGISTRATION_TIMEOUT_MILLIS);
			}
		}, taskExecutor.getMainThreadExecutionContext());

		// upon failure, retry, unless this is cancelled
		resourceManagerFuture.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				if (!isCanceled()) {
					log.warn("Could not resolve ResourceManager address {}, retrying...", resourceManagerAddress);
					resolveResourceManagerAndStartRegistration();
				}
			}
		}, rpcService.getRpcExecutionContext());
	}

	/**
	 * This method performs a registration attempt and triggers either a success notification or a retry,
	 * depending on the result.
	 */
	@SuppressWarnings("unchecked")
	private void register(final ResourceManagerGateway resourceManager, final int attempt, final long timeoutMillis) {
		// this needs to run in the TaskExecutor's main thread
		taskExecutor.validateRunsInMainThread();

		// eager check for canceling to avoid some unnecessary work
		if (canceled) {
			return;
		}

		log.info("Registration at ResourceManager attempt {} (timeout={}ms)", attempt, timeoutMillis);

		FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);

		Future<TaskExecutorRegistrationResponse> registrationFuture = resourceManager.registerTaskExecutor(
				resourceManagerLeaderId, taskExecutor.getAddress(), taskExecutor.getResourceID(),
				taskExecutor.getCurrentSlotReport(), timeout);

		// if the registration was successful, let the TaskExecutor know
		registrationFuture.onSuccess(new OnSuccess<TaskExecutorRegistrationResponse>() {
			
			@Override
			public void onSuccess(TaskExecutorRegistrationResponse result) throws Throwable {
				if (!isCanceled()) {
					if (result instanceof TaskExecutorRegistrationResponse.Success) {
						// registration successful!
						TaskExecutorRegistrationResponse.Success success = 
								(TaskExecutorRegistrationResponse.Success) result;

						connection.completedRegistrationAtResourceManager(
								resourceManager, success.getRegistrationId());
					}
					else {
						// registration refused or unknown
						if (result instanceof TaskExecutorRegistrationResponse.Decline) {
							TaskExecutorRegistrationResponse.Decline decline =
									(TaskExecutorRegistrationResponse.Decline) result;
							log.info("ResourceManager declined this TaskManager: {}", decline.getReason());
						}
						else {
							log.error("Received unknown response from ResourceManager: " + result);
						}
						
						log.info("Pausing and re-attempting registration in {} ms", REFUSED_REGISTRATION_DELAY_MILLIS);

						registerLater(resourceManager, 1, INITIAL_REGISTRATION_TIMEOUT_MILLIS,
								REFUSED_REGISTRATION_DELAY_MILLIS);
					}
				}
			}
		}, taskExecutor.getMainThreadExecutionContext());

		// upon failure, retry
		registrationFuture.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				if (!isCanceled()) {
					if (failure instanceof TimeoutException) {
						// we simply have not received a response in time. maybe the timeout was
						// very low (initial fast registration attempts), maybe the ResourceManager is
						// currently down.
						if (log.isDebugEnabled()) {
							log.debug("Registration at ResourceManager {} attempt {} timed out after {} ms",
									resourceManagerAddress, attempt, timeoutMillis);
						}

						long newTimeoutMillis = Math.min(2 * timeoutMillis, MAX_REGISTRATION_TIMEOUT_MILLIS);
						register(resourceManager, attempt + 1, newTimeoutMillis);
					}
					else {
						// a serious failure occurred. we still should not give up, but keep trying
						log.error("Registration at ResourceManager failed due to an error", failure);
						log.info("Pausing and re-attempting registration in {} ms", ERROR_REGISTRATION_DELAY_MILLIS);

						registerLater(resourceManager, 1, INITIAL_REGISTRATION_TIMEOUT_MILLIS,
								ERROR_REGISTRATION_DELAY_MILLIS);
					}
				}
			}
		}, taskExecutor.getMainThreadExecutionContext());
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private void registerLater(
			final ResourceManagerGateway resourceManager,
			final int attempt,
			final long timeoutMillis,
			long delay) {

		taskExecutor.scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				register(resourceManager, attempt, timeoutMillis);
			}
		}, delay, TimeUnit.MILLISECONDS);
	}
}
