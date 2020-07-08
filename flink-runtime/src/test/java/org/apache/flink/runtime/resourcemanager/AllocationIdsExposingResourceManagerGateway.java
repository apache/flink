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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.ExceptionUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A {@link TestingResourceManagerGateway} that exposes the {@link AllocationID} of all slot requests.
 */
public final class AllocationIdsExposingResourceManagerGateway extends TestingResourceManagerGateway {
	private final BlockingQueue<AllocationID> allocationIds;

	public AllocationIdsExposingResourceManagerGateway() {
		this.allocationIds = new ArrayBlockingQueue<>(10);
		setRequestSlotConsumer(
			slotRequest -> allocationIds.offer(slotRequest.getAllocationId())
		);
	}

	public AllocationID takeAllocationId() {
		try {
			return allocationIds.take();
		} catch (InterruptedException e) {
			ExceptionUtils.rethrow(e);
			return null;
		}
	}
}
