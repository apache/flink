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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Builder for the {@link TestingResourceActions}.
 */
public class TestingResourceActionsBuilder {
	private BiConsumer<InstanceID, Exception> releaseResourceConsumer = (ignoredA, ignoredB) -> {};
	private Function<WorkerResourceSpec, Boolean> allocateResourceFunction = (ignored) -> true;
	private Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer = (ignored) -> {};
	private BiConsumer<JobID, Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer = (ignoredA, ignoredB) -> {};

	public TestingResourceActionsBuilder setReleaseResourceConsumer(BiConsumer<InstanceID, Exception> releaseResourceConsumer) {
		this.releaseResourceConsumer = releaseResourceConsumer;
		return this;
	}

	public TestingResourceActionsBuilder setAllocateResourceFunction(Function<WorkerResourceSpec, Boolean> allocateResourceFunction) {
		this.allocateResourceFunction = allocateResourceFunction;
		return this;
	}

	public TestingResourceActionsBuilder setAllocateResourceConsumer(Consumer<WorkerResourceSpec> allocateResourceConsumer) {
		this.allocateResourceFunction = workerRequest -> {
			allocateResourceConsumer.accept(workerRequest);
			return true;
		};
		return this;
	}

	public TestingResourceActionsBuilder setNotifyAllocationFailureConsumer(Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer) {
		this.notifyAllocationFailureConsumer = notifyAllocationFailureConsumer;
		return this;
	}

	public TestingResourceActionsBuilder setNotEnoughResourcesConsumer(BiConsumer<JobID, Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer) {
		this.notifyNotEnoughResourcesConsumer = notifyNotEnoughResourcesConsumer;
		return this;
	}

	public TestingResourceActions build() {
		return new TestingResourceActions(releaseResourceConsumer, allocateResourceFunction, notifyAllocationFailureConsumer, notifyNotEnoughResourcesConsumer);
	}
}
