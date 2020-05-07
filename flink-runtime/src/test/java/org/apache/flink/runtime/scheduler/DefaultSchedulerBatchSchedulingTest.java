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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

/**
 * Tests for the scheduling of batch jobs with {@link DefaultScheduler}.
 */
public class DefaultSchedulerBatchSchedulingTest extends BatchSchedulingTestBase {

	@Override
	protected DefaultScheduler createScheduler(
			final JobGraph jobGraph,
			final SlotProvider slotProvider,
			final Time slotRequestTimeout) throws Exception {

		return SchedulerTestingUtils.createScheduler(jobGraph, slotProvider, slotRequestTimeout);
	}
}
