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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link SlotRequestBulkManager}.
 */
public class SlotRequestBulkManagerTest extends TestLogger {

	private ManualClock clock = new ManualClock();

	private SlotRequestBulkManager bulkManager;

	@Before
	public void setup() throws Exception {
		bulkManager = new SlotRequestBulkManager(clock);
	}

	@Test
	public void testAddRequestToBulk() {
		final SlotPoolImpl.PendingRequest request = createPendingRequest();
		final long bulkId = 123;
		bulkManager.addRequestToBulk(request, bulkId);

		assertThat(bulkManager.hasBulk(bulkId), is(true));
		assertThat(bulkManager.getSlotRequestBulk(bulkId), contains(request));
		assertThat(bulkManager.getBulkUnfulfillableSince(bulkId), is(clock.relativeTimeMillis()));
	}

	@Test
	public void testRequestWillBeRemovedWhenCompleted() {
		final SlotPoolImpl.PendingRequest request1 = createPendingRequest();
		final SlotPoolImpl.PendingRequest request2 = createPendingRequest();
		final long bulkId = 123;
		bulkManager.addRequestToBulk(request1, bulkId);
		bulkManager.addRequestToBulk(request2, bulkId);

		assertThat(bulkManager.getSlotRequestBulk(bulkId), containsInAnyOrder(request1, request2));

		request1.getAllocatedSlotFuture().completeExceptionally(new Exception("Forced failure"));

		assertThat(bulkManager.getSlotRequestBulk(bulkId), containsInAnyOrder(request2));

		request2.getAllocatedSlotFuture().completeExceptionally(new Exception("Forced failure"));

		assertThat(bulkManager.getSlotRequestBulk(bulkId), is(nullValue()));
	}

	@Test
	public void testUnfulfillableTimestampWillNotBeOverriddenByFollowingUnfulfillableTimestamp() {
		final SlotPoolImpl.PendingRequest request = createPendingRequest();
		final long bulkId = 123;
		bulkManager.addRequestToBulk(request, bulkId);

		final long unfulfillableSince = clock.relativeTimeMillis();

		assertThat(bulkManager.getBulkUnfulfillableSince(bulkId), is(unfulfillableSince));

		clock.advanceTime(456, TimeUnit.MILLISECONDS);
		bulkManager.markBulkUnfulfillable(bulkId, clock.relativeTimeMillis());

		assertThat(bulkManager.getBulkUnfulfillableSince(bulkId), is(unfulfillableSince));

		bulkManager.markBulkFulfillable(bulkId);
		bulkManager.markBulkUnfulfillable(bulkId, clock.relativeTimeMillis());

		assertThat(bulkManager.getBulkUnfulfillableSince(bulkId), is(clock.relativeTimeMillis()));
	}

	private static SlotPoolImpl.PendingRequest createPendingRequest() {
		return SlotPoolImpl.PendingRequest.createStreamingRequest(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN);
	}
}
