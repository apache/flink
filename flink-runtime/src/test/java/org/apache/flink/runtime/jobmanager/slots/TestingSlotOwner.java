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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.runtime.instance.Slot;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * {@link SlotOwner} implementation for testing purposes.
 */
public class TestingSlotOwner implements SlotOwner {

	private volatile Consumer<Slot> returnAllocatedSlotConsumer;

	public void setReturnAllocatedSlotConsumer(Consumer<Slot> returnAllocatedSlotConsumer) {
		this.returnAllocatedSlotConsumer = returnAllocatedSlotConsumer;
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(Slot slot) {
		final Consumer<Slot> currentReturnAllocatedSlotConsumer = this.returnAllocatedSlotConsumer;

		if (currentReturnAllocatedSlotConsumer != null) {
			currentReturnAllocatedSlotConsumer.accept(slot);
		}

		return CompletableFuture.completedFuture(true);
	}
}
