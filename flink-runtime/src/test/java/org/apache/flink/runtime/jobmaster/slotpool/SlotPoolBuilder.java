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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;

import java.util.concurrent.CompletableFuture;

/**
 * Builder for a {@link TestingSlotPoolImpl}.
 */
public class SlotPoolBuilder {

	private ComponentMainThreadExecutor componentMainThreadExecutor;
	private ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
	private Time batchSlotTimeout = Time.milliseconds(2L);
	private Clock clock = SystemClock.getInstance();

	public SlotPoolBuilder(ComponentMainThreadExecutor componentMainThreadExecutor) {
		this.componentMainThreadExecutor = componentMainThreadExecutor;
	}

	public SlotPoolBuilder setResourceManagerGateway(ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = resourceManagerGateway;
		return this;
	}

	public SlotPoolBuilder setBatchSlotTimeout(Time batchSlotTimeout) {
		this.batchSlotTimeout = batchSlotTimeout;
		return this;
	}

	public SlotPoolBuilder setClock(Clock clock) {
		this.clock = clock;
		return this;
	}

	public TestingSlotPoolImpl build() throws Exception {
		final TestingSlotPoolImpl slotPool = new TestingSlotPoolImpl(
			new JobID(),
			clock,
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			batchSlotTimeout);

		slotPool.start(JobMasterId.generate(), "foobar", componentMainThreadExecutor);

		CompletableFuture.runAsync(() -> slotPool.connectToResourceManager(resourceManagerGateway), componentMainThreadExecutor).join();

		return slotPool;
	}
}
