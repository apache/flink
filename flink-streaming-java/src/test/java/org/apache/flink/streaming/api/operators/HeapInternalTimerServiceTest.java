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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link HeapInternalTimerService}.
 */
@RunWith(Parameterized.class)
public class HeapInternalTimerServiceTest extends InternalTimerServiceTestBase {

	public HeapInternalTimerServiceTest(int startKeyGroup, int endKeyGroup, int maxParallelism) {
		super(startKeyGroup, endKeyGroup, maxParallelism);
	}

	@Override
	public InternalTimeServiceManager<Integer, String> createInternalTimeServiceManager(
		int totalKeyGroups,
		KeyGroupRange keyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService
	) throws Exception {
		InternalTimeServiceManager<Integer, String> timeServiceManager =
			new HeapInternalTimeServiceManager<>();

		timeServiceManager.initialize(
				new DummyEnvironment(), new JobID(), "test-op",
				totalKeyGroups, keyGroupRange, IntSerializer.INSTANCE,
				keyContext, processingTimeService);

		return timeServiceManager;
	}
}
