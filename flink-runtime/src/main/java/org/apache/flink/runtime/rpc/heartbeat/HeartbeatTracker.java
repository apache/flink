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

import org.apache.flink.runtime.rpc.Cancellable;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Responsible for tracker the heartbeat from sender at receiver so that if the heartbeat request is not delivered, then receiving end could mark sending end as dead
 */
public abstract class HeartbeatTracker {

	/** default max acceptable heartbeat pause time */
	private static final long DEFAULT_MAX_ACCEPTABLE_HEARTBEAT_PAUSE = 60000;

	/** max acceptable heartbeat pause time */
	private final long maxAcceptableHeartbeatPauseMillis;

	/** heartbeat interval */
	private final long heartbeatIntervalMillis;

	private final RpcService rpcService;

	private final Logger log;

	private volatile long latestHeartbeatTimeStamp;

	/** a ticker to check whether keep heartbeat with receiver periodically */
	private Cancellable heartbeatTicker;

	/**
	 * Creates a new heartbeat tracker at receiver to monitor the heartbeat sender in case lost connection
	 *
	 * @param log
	 * @param rpcService
	 * @param heartbeatIntervalMillis heartbeat interval in millisecond
	 */
	public HeartbeatTracker(Logger log, RpcService rpcService, long heartbeatIntervalMillis) {
		this(log, rpcService, heartbeatIntervalMillis, DEFAULT_MAX_ACCEPTABLE_HEARTBEAT_PAUSE);
	}

	/**
	 * Creates a new heartbeat tracker at receiver to monitor the heartbeat sender in case lost connection
	 *
	 * @param log
	 * @param rpcService
	 * @param heartbeatIntervalMillis           heartbeat interval in millisecond
	 * @param maxAcceptableHeartbeatPauseMillis max acceptable heartbeat pause in millisecond for receiver
	 */
	public HeartbeatTracker(Logger log, RpcService rpcService, long heartbeatIntervalMillis,
		long maxAcceptableHeartbeatPauseMillis)
	{
		checkArgument(heartbeatIntervalMillis > 0, "heartbeat interval must be greater than zero");
		checkArgument(maxAcceptableHeartbeatPauseMillis > heartbeatIntervalMillis, "max acceptable heartbeat pause must be greater than heartbeat interval");
		this.heartbeatIntervalMillis = heartbeatIntervalMillis;
		this.maxAcceptableHeartbeatPauseMillis = maxAcceptableHeartbeatPauseMillis;
		this.log = checkNotNull(log);
		this.rpcService = checkNotNull(rpcService);
	}

	/**
	 * Starts the tracker which will start an inner ticker to check whether can keep heartbeat with sender
	 */
	public void start() {
		checkState(heartbeatTicker == null, "The tracker is already started");
		latestHeartbeatTimeStamp = 0;

		heartbeatTicker = rpcService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long currentTimeMillis = System.currentTimeMillis();
				long timeElapseSinceLastHeartbeat = currentTimeMillis - latestHeartbeatTimeStamp;
				// if cannot receive heartbeat for max acceptable heartbeat pause, it notify the sender is dead
				if (timeElapseSinceLastHeartbeat >= maxAcceptableHeartbeatPauseMillis) {
					log.error("Lost heartbeat for {} ms which is unacceptable.", timeElapseSinceLastHeartbeat);
					notifyOfLostHeartbeatWithSender();
				}
			}
		}, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
	}

	/**
	 * Cancel the tracker which will stop the inner ticker
	 */
	public void cancel() {
		if (heartbeatTicker != null) {
			heartbeatTicker.cancel();
			heartbeatTicker = null;
		}
	}

	/**
	 * Notify receive heartbeat message from Sender
	 */
	public void notifyOfReceivingHeartbeat() {
		latestHeartbeatTimeStamp = System.currentTimeMillis();
	}

	/**
	 * Notify lost of heartbeat with sender
	 */
	protected abstract void notifyOfLostHeartbeatWithSender();

}
