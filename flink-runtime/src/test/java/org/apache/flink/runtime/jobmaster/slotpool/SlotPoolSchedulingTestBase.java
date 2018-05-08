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
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Test base for {@link SlotPool} related scheduling test cases.
 */
@Category(New.class)
public class SlotPoolSchedulingTestBase extends TestLogger {

	private static final JobID jobId = new JobID();

	private static final JobMasterId jobMasterId = JobMasterId.generate();

	private static final String jobMasterAddress = "foobar";

	private static TestingRpcService testingRpcService;

	protected SlotPool slotPool;

	protected SlotPoolGateway slotPoolGateway;

	protected SlotProvider slotProvider;

	protected TestingResourceManagerGateway testingResourceManagerGateway;

	@BeforeClass
	public static void setup() {
		testingRpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardown() throws ExecutionException, InterruptedException {
		if (testingRpcService != null) {
			testingRpcService.stopService().get();
			testingRpcService = null;
		}
	}

	@Before
	public void setupBefore() throws Exception {
		testingResourceManagerGateway = new TestingResourceManagerGateway();

		slotPool = new SlotPool(
			testingRpcService,
			jobId);

		slotPool.start(jobMasterId, jobMasterAddress);

		slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

		slotProvider = slotPool.getSlotProvider();

		slotPool.connectToResourceManager(testingResourceManagerGateway);
	}

	@After
	public void teardownAfter() throws InterruptedException, ExecutionException, TimeoutException {
		if (slotPool != null) {
			RpcUtils.terminateRpcEndpoint(slotPool, TestingUtils.TIMEOUT());
			slotPool = null;
		}
	}
}
