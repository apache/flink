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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;

import org.junit.rules.ExternalResource;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/**
 * {@link ExternalResource} which provides a {@link SlotPool}.
 */
public class SlotPoolResource extends ExternalResource {

	@Nonnull
	private final RpcService rpcService;

	@Nonnull
	private final SchedulingStrategy schedulingStrategy;

	private SlotPool slotPool;

	private SlotPoolGateway slotPoolGateway;

	private TestingResourceManagerGateway testingResourceManagerGateway;

	public SlotPoolResource(@Nonnull RpcService rpcService, @Nonnull SchedulingStrategy schedulingStrategy) {
		this.rpcService = rpcService;
		this.schedulingStrategy = schedulingStrategy;
		slotPool = null;
		slotPoolGateway = null;
		testingResourceManagerGateway = null;
	}

	public SlotProvider getSlotProvider() {
		checkInitialized();
		return slotPool.getSlotProvider();
	}

	public TestingResourceManagerGateway getTestingResourceManagerGateway() {
		checkInitialized();
		return testingResourceManagerGateway;
	}

	public SlotPoolGateway getSlotPoolGateway() {
		checkInitialized();
		return slotPoolGateway;
	}

	private void checkInitialized() {
		assert(slotPool != null);
	}

	@Override
	protected void before() throws Throwable {
		if (slotPool != null) {
			terminateSlotPool();
		}

		testingResourceManagerGateway = new TestingResourceManagerGateway();

		slotPool = new SlotPool(
			rpcService,
			new JobID(),
			schedulingStrategy);

		slotPool.start(JobMasterId.generate(), "foobar");

		slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

		slotPool.connectToResourceManager(testingResourceManagerGateway);
	}

	@Override
	protected void after() {
		if (slotPool != null) {
			terminateSlotPool();
			slotPool = null;
		}
	}

	private void terminateSlotPool() {
		slotPool.shutDown();
		CompletableFuture<Void> terminationFuture = slotPool.getTerminationFuture();
		terminationFuture.join();
	}
}
