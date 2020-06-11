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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Testing extension of the {@link SlotPoolImpl} which adds additional methods
 * for testing.
 */
public class TestingSlotPoolImpl extends SlotPoolImpl {

	private ResourceProfile lastRequestedSlotResourceProfile;

	public TestingSlotPoolImpl(JobID jobId) {
		this(
			jobId,
			SystemClock.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			AkkaUtils.getDefaultTimeout(),
			Time.milliseconds(JobManagerOptions.SLOT_IDLE_TIMEOUT.defaultValue()));
	}

	public TestingSlotPoolImpl(
			JobID jobId,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout) {
		super(jobId, clock, rpcTimeout, idleSlotTimeout, batchSlotTimeout);
	}

	void triggerCheckIdleSlot() {
		runAsync(this::checkIdleSlot);
	}

	void triggerCheckBatchSlotTimeout() {
		runAsync(this::checkBatchSlotTimeout);
	}

	@Override
	public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			final SlotRequestId slotRequestId,
			final ResourceProfile resourceProfile,
			@Nullable final Time timeout) {

		this.lastRequestedSlotResourceProfile = resourceProfile;

		return super.requestNewAllocatedSlot(slotRequestId, resourceProfile, timeout);
	}

	public ResourceProfile getLastRequestedSlotResourceProfile() {
		return lastRequestedSlotResourceProfile;
	}
}
