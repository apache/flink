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

package org.apache.flink.runtime.rpc.heartbeat;

import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This utility class implements the basis of schedule heartbeat from one component to another component periodically,
 * for example trigger heartbeat from the ResourceManager to TaskExecutor.
 *
 * @param <Gateway> The type of the gateway to connect to.
 * @param <Payload> The type of the successful heartbeat responses with payload.
 */
public abstract class HeartbeatScheduler<Gateway extends RpcGateway, Payload extends Serializable> {
	/** default heartbeat interval time in millisecond */
	private static final long INITIAL_HEARTBEAT_INTERVAL_MILLIS = 5000;

	/** default heartbeat timeout in millisecond */
	private static final long INITIAL_HEARTBEAT_TIMEOUT_MILLIS = 200;

	/** default max heartbeat interval time in millisecond (which is used in retry heartbeat case) */
	private static final long MAX_HEARTBEAT_TIMEOUT_MILLIS = 30000;

	/** default heartbeat attempt delay after an exception has occurred */
	private static final long ERROR_HEARTBEAT_DELAY_MILLIS = 2000;

	/** default max heartbeat retry time for one heartbeat */
	private static final long MAX_HEARTBEAT_ATTEMPT_MILLIS = 60000;

	private final long heartbeatInterval;

	private final long heartbeatTimeout;

	private final long maxHeartbeatTimeout;

	private final long delayOnError;

	private final long maxAttemptTime;

	/** target gateway to receive the heartbeat and give heartbeatResponse */
	protected final Gateway targetGateway;

	/** the target address */
	private final String targetAddress;

	/** the target gateway name */
	private final String targetName;

	private final RpcService rpcService;

	private final UUID leaderID;

	private final Logger log;

	private volatile boolean closed;

	/**
	 * @param rpcService    rpcService
	 * @param leaderID      leader session id of current source end which send heartbeat
	 * @param targetGateway target gateway which receive heartbeat and response
	 * @param targetAddress target gateway address
	 * @param targetName    target name
	 * @param log           log
	 */
	public HeartbeatScheduler(RpcService rpcService, UUID leaderID, Gateway targetGateway,
		String targetAddress, String targetName, Logger log) {
		this(rpcService, leaderID, targetGateway, targetAddress, targetName, log, INITIAL_HEARTBEAT_INTERVAL_MILLIS,
			INITIAL_HEARTBEAT_TIMEOUT_MILLIS, MAX_HEARTBEAT_TIMEOUT_MILLIS, ERROR_HEARTBEAT_DELAY_MILLIS, MAX_HEARTBEAT_ATTEMPT_MILLIS);
	}

	/**
	 * @param rpcService          rpcService
	 * @param leaderID            leader session id of current source end which send heartbeat
	 * @param targetGateway       target gateway which receive heartbeat and response
	 * @param targetAddress       target gateway address
	 * @param targetName          target name
	 * @param log                 log
	 * @param heartbeatInterval   heartbeat interval time in millisecond
	 * @param heartbeatTimeout    heartbeat timeout in millisecond
	 * @param maxHeartbeatTimeout max heartbeat interval time in millisecond
	 * @param delayOnError        Heartbeat attempt delay after an exception has occurred
	 * @param maxAttemptTime      max retry time for one heartbeat
	 */
	public HeartbeatScheduler(
		RpcService rpcService, UUID leaderID, Gateway targetGateway,
		String targetAddress, String targetName, Logger log, long heartbeatInterval,
		long heartbeatTimeout, long maxHeartbeatTimeout, long delayOnError, long maxAttemptTime) {
		checkArgument(heartbeatInterval > 0, "initial heartbeat interval must be greater than zero");
		checkArgument(heartbeatTimeout > 0, "initial heartbeat timeout must be greater than zero");
		checkArgument(maxHeartbeatTimeout > 0, "maximum heartbeat timeout must be greater than zero");
		checkArgument(delayOnError >= 0, "delay on error must be non-negative");
		checkArgument(maxAttemptTime >= 0, "max attempt on error must be non-negative");
		this.rpcService = checkNotNull(rpcService);
		this.leaderID = checkNotNull(leaderID);
		this.targetGateway = checkNotNull(targetGateway);
		this.targetAddress = checkNotNull(targetAddress);
		this.targetName = checkNotNull(targetName);
		this.log = checkNotNull(log);
		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatTimeout = heartbeatTimeout;
		this.maxHeartbeatTimeout = maxHeartbeatTimeout;
		this.delayOnError = delayOnError;
		this.maxAttemptTime = maxAttemptTime;
	}

	/**
	 * Start to schedule heartbeat
	 */
	public void start() {
		checkState(!closed, "The heartbeat connection is already closed");
		long currentHeartbeatBeginTime = System.currentTimeMillis();
		sendHeartbeatToTaskManager(1, heartbeatTimeout, currentHeartbeatBeginTime);
	}

	/**
	 * Checks if the heartbeat schedule was closed.
	 *
	 * @return True if the heartbeat schedule was closed, false otherwise.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Stop to schedule heartbeat
	 */
	public void close() {
		closed = true;
	}

	/**
	 * Get the heartbeat interval
	 *
	 * @return heartbeat interval
	 */
	public long getHeartbeatInterval() {
		return heartbeatInterval;
	}

	/**
	 * Trigger heartbeat to target gateway
	 *
	 * @param leaderID leader session id of current sender
	 * @param timeout  timeout for heartbeat response
	 * @return HeartbeatResponsePayload wrapped in future
	 */
	protected abstract Future<Payload> triggerHeartbeat(UUID leaderID, FiniteDuration timeout);

	/**
	 * Report heartbeat response payload to sender who sending heartbeat
	 *
	 * @param heartbeatResponsePayload heartbeat response which contains payload
	 */
	protected abstract void reportHeartbeatPayload(Payload heartbeatResponsePayload);

	/**
	 * Callback method when heartbeat sender lost heartbeat with target
	 */
	protected abstract void lostHeartbeat();

	/**
	 * Send a heartbeat attempt to target, receive the response from target or failed depends on the future result.
	 *
	 * @param attempt                   current attempt time
	 * @param timeoutMillis             heartbeat timeout in millisecond
	 * @param currentHeartbeatBeginTime begin time of current heartbeat
	 */
	private void sendHeartbeatToTaskManager(final int attempt, final long timeoutMillis,
		final long currentHeartbeatBeginTime) {
		// eager check for closed to avoid some unnecessary work
		if (closed) {
			return;
		}
		FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);
		Future<Payload> heartbeatResponse = triggerHeartbeat(leaderID, timeout);

		heartbeatResponse.onSuccess(new OnSuccess<Payload>() {

			@Override
			public void onSuccess(Payload result) throws Throwable {
				if (!isClosed()) {
					// report heartbeat response payload back to sender
					reportHeartbeatPayload(result);
					scheduleNewHeartbeatToTaskManagerLater(heartbeatTimeout, heartbeatInterval);
				}
			}
		}, rpcService.getExecutionContext());

		// upon failure, retry
		heartbeatResponse.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				if (!isClosed()) {
					long currentTime = System.currentTimeMillis();
					if (currentTime - currentHeartbeatBeginTime >= maxAttemptTime) {
						log.error("Lost heartbeat with at {} ({}) after {} attempts", targetName, targetAddress, attempt);
						closed = true;
						// mark target as failed after heartbeat interaction attempts failed for max attempt time
						lostHeartbeat();
					} else {
						// we simply have not received a heartbeat response in time. maybe the timeout was
						// very low (initial fast registration attempts), maybe the target endpoint is
						// currently down.
						if (failure instanceof TimeoutException) {
							if (log.isDebugEnabled()) {
								log.debug("Heartbeat to {} ({}) attempt {} timed out after {} ms",
									targetName, targetAddress, attempt, timeoutMillis);
							}

							long retryTimeoutMillis = Math.min(2 * timeoutMillis, maxHeartbeatTimeout);
							sendHeartbeatToTaskManager(attempt + 1, retryTimeoutMillis, currentHeartbeatBeginTime);
						} else {
							// a serious failure occurred. we still should not give up, but keep trying
							log.error("Heartbeat to " + targetName + " failed due to an error", failure);
							log.info("Pausing and re-attempting registration in {} ms", delayOnError);

							sendHeartbeatToTaskManagerLater(attempt + 1, timeoutMillis, delayOnError, currentHeartbeatBeginTime);
						}
					}
				}
			}
		}, rpcService.getExecutionContext());
	}

	/**
	 * Schedule a new heartbeat after delayMills
	 *
	 * @param timeoutMillis heartbeat timeout in millisecond
	 * @param delayMillis   delay in millisecond to schedule new heartbeat
	 */
	private void scheduleNewHeartbeatToTaskManagerLater(final long timeoutMillis, final long delayMillis) {
		rpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				long currentHeartbeatBeginTime = System.currentTimeMillis();
				sendHeartbeatToTaskManager(1, timeoutMillis, currentHeartbeatBeginTime);
			}
		}, delayMillis, TimeUnit.MILLISECONDS);
	}

	/**
	 * Retry to sendHeartbeatToTaskManager after a few milliseconds
	 *
	 * @param attempt                   current attempt time
	 * @param timeoutMillis             heartbeat timeout in millisecond
	 * @param delayMills                delay in millisecond to schedule new heartbeat
	 * @param currentHeartbeatBeginTime begin time of current heartbeat
	 */
	private void sendHeartbeatToTaskManagerLater(final int attempt, final long timeoutMillis, final long delayMills,
		final long currentHeartbeatBeginTime) {
		rpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				sendHeartbeatToTaskManager(attempt, timeoutMillis, currentHeartbeatBeginTime);
			}
		}, delayMills, TimeUnit.MILLISECONDS);
	}
}
