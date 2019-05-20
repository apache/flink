/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nonnull;

/**
 * Default implementation of a {@link SchedulerFactory}.
 */
public class DefaultSchedulerFactory implements SchedulerFactory {

	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	public DefaultSchedulerFactory(@Nonnull SlotSelectionStrategy slotSelectionStrategy) {
		this.slotSelectionStrategy = slotSelectionStrategy;
	}

	@Nonnull
	@Override
	public Scheduler createScheduler(@Nonnull SlotPool slotPool) {
		return new SchedulerImpl(slotSelectionStrategy, slotPool);
	}

	@Nonnull
	private static SlotSelectionStrategy selectSlotSelectionStrategy(@Nonnull Configuration configuration) {
		if (configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY)) {
			return PreviousAllocationSlotSelectionStrategy.INSTANCE;
		} else {
			return LocationPreferenceSlotSelectionStrategy.INSTANCE;
		}
	}

	public static DefaultSchedulerFactory fromConfiguration(
		@Nonnull Configuration configuration) {
		return new DefaultSchedulerFactory(selectSlotSelectionStrategy(configuration));
	}
}
