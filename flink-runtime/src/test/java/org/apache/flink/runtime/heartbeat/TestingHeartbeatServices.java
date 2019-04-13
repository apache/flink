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
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

/**
 * A {@link HeartbeatServices} that allows the injection of a {@link ScheduledExecutor}.
 */
public class TestingHeartbeatServices extends HeartbeatServices {

	private final ScheduledExecutor scheduledExecutorToUse;

	public TestingHeartbeatServices(long heartbeatInterval, long heartbeatTimeout, ScheduledExecutor scheduledExecutorToUse) {
		super(heartbeatInterval, heartbeatTimeout);

		this.scheduledExecutorToUse = Preconditions.checkNotNull(scheduledExecutorToUse);
	}

	public TestingHeartbeatServices() {
		this(1000L, 10000L, TestingUtils.defaultScheduledExecutor());
	}

	@Override
	public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
		ResourceID resourceId,
		HeartbeatListener<I, O> heartbeatListener,
		ScheduledExecutor scheduledExecutor,
		Logger log) {

		return new HeartbeatManagerSenderImpl<>(
			heartbeatInterval,
			heartbeatTimeout,
			resourceId,
			heartbeatListener,
			org.apache.flink.runtime.concurrent.Executors.directExecutor(),
			scheduledExecutorToUse,
			log);
	}
}
