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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;

import javax.annotation.Nonnull;

/**
 * Default slot pool factory.
 */
public class DefaultSlotPoolFactory implements SlotPoolFactory {

	@Nonnull
	private final Clock clock;

	@Nonnull
	private final Time rpcTimeout;

	@Nonnull
	private final Time slotIdleTimeout;

	@Nonnull
	private final Time batchSlotTimeout;

	public DefaultSlotPoolFactory(
			@Nonnull Clock clock,
			@Nonnull Time rpcTimeout,
			@Nonnull Time slotIdleTimeout,
			@Nonnull Time batchSlotTimeout) {
		this.clock = clock;
		this.rpcTimeout = rpcTimeout;
		this.slotIdleTimeout = slotIdleTimeout;
		this.batchSlotTimeout = batchSlotTimeout;
	}

	@Override
	@Nonnull
	public SlotPool createSlotPool(@Nonnull JobID jobId) {
		return new SlotPoolImpl(
			jobId,
			clock,
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout);
	}

	public static DefaultSlotPoolFactory fromConfiguration(@Nonnull Configuration configuration) {

		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		final Time slotIdleTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
		final Time batchSlotTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

		return new DefaultSlotPoolFactory(
			SystemClock.getInstance(),
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout);
	}
}
