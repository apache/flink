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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import scala.concurrent.duration.Duration;

/**
 * Configuration for the {@link SlotManager}.
 */
public class SlotManagerConfiguration {

	private final Time taskManagerRequestTimeout;
	private final Time slotRequestTimeout;
	private final Time taskManagerTimeout;
	private final Time taskManagerCheckerInitialDelay;

	public SlotManagerConfiguration(
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout,
			Time taskManagerCheckerInitialDelay) {
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
		this.taskManagerCheckerInitialDelay = Preconditions.checkNotNull(taskManagerCheckerInitialDelay);
	}

	public Time getTaskManagerRequestTimeout() {
		return taskManagerRequestTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public Time getTaskManagerTimeout() {
		return taskManagerTimeout;
	}

	public Time getTaskManagerCheckerInitialDelay() {
		return taskManagerCheckerInitialDelay;
	}

	public static SlotManagerConfiguration fromConfiguration(Configuration configuration) throws ConfigurationException {
		final String strTimeout = configuration.getString(AkkaOptions.ASK_TIMEOUT);
		final Time rpcTimeout;

		try {
			rpcTimeout = Time.milliseconds(Duration.apply(strTimeout).toMillis());
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Could not parse the resource manager's timeout " +
				"value " + AkkaOptions.ASK_TIMEOUT + '.', e);
		}

		final Time slotRequestTimeout = Time.milliseconds(
				configuration.getLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT));
		final Time taskManagerTimeout = Time.milliseconds(
				configuration.getLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT));
		final Time taskManagerCheckerInitialDelay = Time.milliseconds(
			configuration.getLong(ResourceManagerOptions.TASK_MANAGER_CHECKER_INITIAL_DELAY));

		return new SlotManagerConfiguration(rpcTimeout, slotRequestTimeout, taskManagerTimeout, taskManagerCheckerInitialDelay);
	}
}
