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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

/**
 * HeartbeatServices gives access to all services needed for heartbeating. This includes the
 * creation of heartbeat receivers and heartbeat senders.
 */
public class HeartbeatServices {

	/** Heartbeat interval for the created services. */
	protected final long heartbeatInterval;

	/** Heartbeat timeout for the created services. */
	protected final long heartbeatTimeout;

	public HeartbeatServices(long heartbeatInterval, long heartbeatTimeout) {
		Preconditions.checkArgument(0L < heartbeatInterval, "The heartbeat interval must be larger than 0.");
		Preconditions.checkArgument(heartbeatInterval <= heartbeatTimeout, "The heartbeat timeout should be larger or equal than the heartbeat interval.");

		this.heartbeatInterval = heartbeatInterval;
		this.heartbeatTimeout = heartbeatTimeout;
	}

	/**
	 * Creates a heartbeat manager which does not actively send heartbeats.
	 *
	 * @param resourceId Resource Id which identifies the owner of the heartbeat manager
	 * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for registered
	 *                          targets
	 * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts
	 * @param log Logger to be used for the logging
	 * @param <I> Type of the incoming payload
	 * @param <O> Type of the outgoing payload
	 * @return A new HeartbeatManager instance
	 */
	public <I, O> HeartbeatManager<I, O> createHeartbeatManager(
		ResourceID resourceId,
		HeartbeatListener<I, O> heartbeatListener,
		ScheduledExecutor mainThreadExecutor,
		Logger log) {

		return new HeartbeatManagerImpl<>(
			heartbeatTimeout,
			resourceId,
			heartbeatListener,
			mainThreadExecutor,
			log);
	}

	/**
	 * Creates a heartbeat manager which actively sends heartbeats to monitoring targets.
	 *
	 * @param resourceId Resource Id which identifies the owner of the heartbeat manager
	 * @param heartbeatListener Listener which will be notified upon heartbeat timeouts for registered
	 *                          targets
	 * @param mainThreadExecutor Scheduled executor to be used for scheduling heartbeat timeouts and
	 *                           periodically send heartbeat requests
	 * @param log Logger to be used for the logging
	 * @param <I> Type of the incoming payload
	 * @param <O> Type of the outgoing payload
	 * @return A new HeartbeatManager instance which actively sends heartbeats
	 */
	public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
		ResourceID resourceId,
		HeartbeatListener<I, O> heartbeatListener,
		ScheduledExecutor mainThreadExecutor,
		Logger log) {

		return new HeartbeatManagerSenderImpl<>(
			heartbeatInterval,
			heartbeatTimeout,
			resourceId,
			heartbeatListener,
			mainThreadExecutor,
			log);
	}

	/**
	 * Creates an HeartbeatServices instance from a {@link Configuration}.
	 *
	 * @param configuration Configuration to be used for the HeartbeatServices creation
	 * @return An HeartbeatServices instance created from the given configuration
	 */
	public static HeartbeatServices fromConfiguration(Configuration configuration) {
		long heartbeatInterval = configuration.getLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL);

		long heartbeatTimeout = configuration.getLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT);

		return new HeartbeatServices(heartbeatInterval, heartbeatTimeout);
	}
}
