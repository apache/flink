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

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Testing implementation of the {@link ResourceActions}.
 */
public class TestingResourceActions implements ResourceActions {

	@Nonnull
	private final BiConsumer<InstanceID, Exception> releaseResourceConsumer;

	@Nonnull
	private final Function<WorkerResourceSpec, Boolean> allocateResourceFunction;

	@Nonnull
	private final Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer;

	@Nonnull
	private final BiConsumer<JobID, Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer;

	public TestingResourceActions(
			@Nonnull BiConsumer<InstanceID, Exception> releaseResourceConsumer,
			@Nonnull Function<WorkerResourceSpec, Boolean> allocateResourceFunction,
			@Nonnull Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer,
			@Nonnull BiConsumer<JobID, Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer) {
		this.releaseResourceConsumer = releaseResourceConsumer;
		this.allocateResourceFunction = allocateResourceFunction;
		this.notifyAllocationFailureConsumer = notifyAllocationFailureConsumer;
		this.notifyNotEnoughResourcesConsumer = notifyNotEnoughResourcesConsumer;
	}

	@Override
	public void releaseResource(InstanceID instanceId, Exception cause) {
		releaseResourceConsumer.accept(instanceId, cause);
	}

	@Override
	public boolean allocateResource(WorkerResourceSpec workerResourceSpec){
		return allocateResourceFunction.apply(workerResourceSpec);
	}

	@Override
	public void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause) {
		notifyAllocationFailureConsumer.accept(Tuple3.of(jobId, allocationId, cause));
	}

	@Override
	public void notifyNotEnoughResourcesAvailable(JobID jobId, Collection<ResourceRequirement> acquiredResources) {
		notifyNotEnoughResourcesConsumer.accept(jobId, acquiredResources);
	}
}
