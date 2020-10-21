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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nonnull;

/**
 * Factory interface for {@link SlotPool}.
 */
public interface SlotPoolFactory {

	@Nonnull
	SlotPool createSlotPool(@Nonnull JobID jobId);

	static SlotPoolFactory fromConfiguration(Configuration configuration) {
		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		final Time slotIdleTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
		final Time batchSlotTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

		if (ClusterOptions.isDeclarativeResourceManagementEnabled(configuration)) {

			return new DeclarativeSlotPoolBridgeFactory(
				SystemClock.getInstance(),
				rpcTimeout,
				slotIdleTimeout,
				batchSlotTimeout
			);
		} else {
			return new DefaultSlotPoolFactory(
				SystemClock.getInstance(),
				rpcTimeout,
				slotIdleTimeout,
				batchSlotTimeout);
		}
	}
}
