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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Simple {@link AllocatedSlotActions} implementations for testing purposes.
 */
public class TestingAllocatedSlotActions implements AllocatedSlotActions {

	private volatile Consumer<Tuple3<SlotRequestId, SlotSharingGroupId, Throwable>> releaseSlotConsumer;

	public void setReleaseSlotConsumer(@Nullable Consumer<Tuple3<SlotRequestId, SlotSharingGroupId, Throwable>> releaseSlotConsumer) {
		this.releaseSlotConsumer = releaseSlotConsumer;
	}

	@Override
	public CompletableFuture<Acknowledge> releaseSlot(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, @Nullable Throwable cause) {
		Consumer<Tuple3<SlotRequestId, SlotSharingGroupId, Throwable>> currentReleaseSlotConsumer = this.releaseSlotConsumer;

		if (currentReleaseSlotConsumer != null) {
			currentReleaseSlotConsumer.accept(Tuple3.of(slotRequestId, slotSharingGroupId, cause));
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}
}
