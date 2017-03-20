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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.slf4j.Logger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * Heartbeat manager implementation which extends {@link HeartbeatManagerImpl} for testing.
 * It overrides the {@link #unmonitorTarget(ResourceID)} to wait for some tests complete
 * when notify heartbeat timeout.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
public class TestingHeartbeatManagerImpl<I, O> extends HeartbeatManagerImpl<I, O> {

	private final CountDownLatch waitLatch;

	public TestingHeartbeatManagerImpl(
			CountDownLatch waitLatch,
			long heartbeatTimeoutIntervalMs,
			ResourceID ownResourceID,
			HeartbeatListener<I, O> heartbeatListener,
			Executor executor,
			ScheduledExecutor scheduledExecutor,
			Logger log) {

		super(heartbeatTimeoutIntervalMs, ownResourceID, heartbeatListener, executor, scheduledExecutor, log);

		this.waitLatch = waitLatch;
	}

	@Override
	public void unmonitorTarget(ResourceID resourceID) {
		try {
			waitLatch.await();
		} catch (InterruptedException ex) {
			log.error("Unexpected interrupted exception.", ex);
		}

		super.unmonitorTarget(resourceID);
	}
}
