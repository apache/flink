/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Builder for {@link DefaultDeclarativeSlotPool}.
 */
final class DefaultDeclarativeSlotPoolBuilder {

	private AllocatedSlotPool allocatedSlotPool = new DefaultAllocatedSlotPool();
	private Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements = ignored -> {};
	private Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots = ignored -> {};
	private Time idleSlotTimeout = Time.seconds(20);
	private Time rpcTimeout = Time.seconds(20);

	public DefaultDeclarativeSlotPoolBuilder setAllocatedSlotPool(AllocatedSlotPool allocatedSlotPool) {
		this.allocatedSlotPool = allocatedSlotPool;
		return this;
	}

	public DefaultDeclarativeSlotPoolBuilder setNotifyNewResourceRequirements(Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements) {
		this.notifyNewResourceRequirements = notifyNewResourceRequirements;
		return this;
	}

	public DefaultDeclarativeSlotPoolBuilder setNotifyNewSlots(Consumer<? super Collection<? extends PhysicalSlot>> notifyNewSlots) {
		this.notifyNewSlots = notifyNewSlots;
		return this;
	}

	public DefaultDeclarativeSlotPoolBuilder setIdleSlotTimeout(Time idleSlotTimeout) {
		this.idleSlotTimeout = idleSlotTimeout;
		return this;
	}

	public DefaultDeclarativeSlotPool build() {
		return new DefaultDeclarativeSlotPool(
				allocatedSlotPool,
				notifyNewResourceRequirements,
				notifyNewSlots,
				idleSlotTimeout,
				rpcTimeout);
	}

	public static DefaultDeclarativeSlotPoolBuilder builder() {
		return new DefaultDeclarativeSlotPoolBuilder();
	}
}
