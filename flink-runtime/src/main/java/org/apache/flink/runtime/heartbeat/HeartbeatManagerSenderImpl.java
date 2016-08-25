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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from
 * its monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoind heartbeat payload
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O> implements Runnable {

	private final ScheduledFuture<?> triggerFuture;

	public HeartbeatManagerSenderImpl(
		long heartbeatPeriod,
		long heartbeatTimeout,
		ResourceID ownResourceID,
		ExecutorService executorService,
		ScheduledExecutorService scheduledExecutorService,
		Logger log) {
		super(heartbeatTimeout, ownResourceID, executorService, scheduledExecutorService, log);

		triggerFuture = scheduledExecutorService.scheduleAtFixedRate(this, 0L, heartbeatPeriod, TimeUnit.MILLISECONDS);
	}

	@Override
	public void run() {
		if (!stopped) {
			log.debug("Trigger heartbeat request.");
			for (HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets()) {
				Future<O> futurePayload = getHeartbeatListener().retrievePayload();
				final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();

				if (futurePayload != null) {
					futurePayload.thenAcceptAsync(new AcceptFunction<O>() {
						@Override
						public void accept(O payload) {
							heartbeatTarget.requestHeartbeat(getOwnResourceID(), payload);
						}
					}, getExecutor());
				} else {
					heartbeatTarget.requestHeartbeat(getOwnResourceID(), null);
				}
			}
		}
	}

	@Override
	public void stop() {
			triggerFuture.cancel(true);
			super.stop();
	}
}
