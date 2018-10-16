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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSchedulingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolSlotProvider;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;

/**
 * Test base for scheduler related test cases. The test are
 * executed with the {@link SlotPool}.
 */
public class SchedulerTestBase extends TestLogger {

	protected TestingSlotPoolSlotProvider testingSlotProvider;

	private RpcService rpcService;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();
		final JobID jobId = new JobID();
		final TestingSlotPool slotPool = new TestingSlotPool(
			rpcService,
			jobId,
			LocationPreferenceSchedulingStrategy.getInstance());
		testingSlotProvider = new TestingSlotPoolSlotProvider(slotPool);

		final JobMasterId jobMasterId = JobMasterId.generate();
		final String jobManagerAddress = "localhost";
		slotPool.start(jobMasterId, jobManagerAddress);
	}

	@After
	public void teardown() throws Exception {
		if (testingSlotProvider != null) {
			testingSlotProvider.shutdown();
			testingSlotProvider = null;
		}

		if (rpcService != null) {
			rpcService.stopService().get();
			rpcService = null;
		}
	}
}
