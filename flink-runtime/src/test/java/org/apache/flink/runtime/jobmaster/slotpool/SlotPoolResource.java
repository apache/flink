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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;

import org.junit.rules.ExternalResource;

import javax.annotation.Nonnull;

/**
 * {@link ExternalResource} which provides a {@link SlotPoolImpl}.
 */
public class SlotPoolResource extends ExternalResource {

	@Nonnull
	private final SlotSelectionStrategy schedulingStrategy;

	private SlotPoolImpl slotPool;

	private Scheduler scheduler;

	private TestingResourceManagerGateway testingResourceManagerGateway;

	private final ComponentMainThreadExecutor mainThreadExecutor;

	public SlotPoolResource(@Nonnull SlotSelectionStrategy schedulingStrategy) {
		this.schedulingStrategy = schedulingStrategy;
		this.mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
		slotPool = null;
		testingResourceManagerGateway = null;
	}

	public SlotProvider getSlotProvider() {
		checkInitialized();
		return scheduler;
	}

	public TestingResourceManagerGateway getTestingResourceManagerGateway() {
		checkInitialized();
		return testingResourceManagerGateway;
	}

	public SlotPoolImpl getSlotPool() {
		checkInitialized();
		return slotPool;
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

		slotPool = new TestingSlotPoolImpl(new JobID());
		scheduler = new SchedulerImpl(schedulingStrategy, slotPool);
		slotPool.start(JobMasterId.generate(), "foobar", mainThreadExecutor);
		scheduler.start(mainThreadExecutor);
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
		slotPool.close();
	}
}
