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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.testingUtils.TestingUtils;

/** Builder for {@link SlotManagerImpl}. */
public class SlotManagerBuilder {
	private ScheduledExecutor scheduledExecutor;
	private Time taskManagerRequestTimeout;
	private Time slotRequestTimeout;
	private Time taskManagerTimeout;
	private boolean waitResultConsumedBeforeRelease;

	private SlotManagerBuilder() {
		this.scheduledExecutor = TestingUtils.defaultScheduledExecutor();
		this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
		this.slotRequestTimeout = TestingUtils.infiniteTime();
		this.taskManagerTimeout = TestingUtils.infiniteTime();
		this.waitResultConsumedBeforeRelease = true;
	}

	public static SlotManagerBuilder newBuilder() {
		return new SlotManagerBuilder();
	}

	public SlotManagerBuilder setScheduledExecutor(ScheduledExecutor scheduledExecutor) {
		this.scheduledExecutor = scheduledExecutor;
		return this;
	}

	public SlotManagerBuilder setTaskManagerRequestTimeout(Time taskManagerRequestTimeout) {
		this.taskManagerRequestTimeout = taskManagerRequestTimeout;
		return this;
	}

	public SlotManagerBuilder setSlotRequestTimeout(Time slotRequestTimeout) {
		this.slotRequestTimeout = slotRequestTimeout;
		return this;
	}

	public SlotManagerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
		this.taskManagerTimeout = taskManagerTimeout;
		return this;
	}

	public SlotManagerBuilder setWaitResultConsumedBeforeRelease(boolean waitResultConsumedBeforeRelease) {
		this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
		return this;
	}

	public SlotManagerImpl build() {
		return new SlotManagerImpl(
			scheduledExecutor,
			taskManagerRequestTimeout,
			slotRequestTimeout,
			taskManagerTimeout,
			waitResultConsumedBeforeRelease);
	}
}
