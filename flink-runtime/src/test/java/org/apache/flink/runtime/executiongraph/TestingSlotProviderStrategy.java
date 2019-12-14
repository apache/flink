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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

/**
 * Testing implementation of the {@link SlotProviderStrategy} which uses {@link ScheduleMode#LAZY_FROM_SOURCES}
 * and sets the allocation timeout to 10s.
 */
public class TestingSlotProviderStrategy extends SlotProviderStrategy.NormalSlotProviderStrategy{

	private TestingSlotProviderStrategy(SlotProvider slotProvider, Time allocationTimeout) {
		super(slotProvider, allocationTimeout);
	}

	public static TestingSlotProviderStrategy from(SlotProvider slotProvider) {
		return new TestingSlotProviderStrategy(slotProvider, Time.seconds(10L));
	}
}
