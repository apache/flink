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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.createPhysicalSlot;
import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.occupyPhysicalSlot;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link PhysicalSlotRequestBulkChecker}.
 */
public class PhysicalSlotRequestBulkCheckerTest extends TestLogger {

	private static final Time TIMEOUT = Time.milliseconds(5000L);

	private final ManualClock clock = new ManualClock();

	private PhysicalSlotRequestBulkChecker bulkChecker;

	private Set<PhysicalSlot> slots;

	private Supplier<Set<SlotInfo>> slotsRetriever;

	@Before
	public void setup() throws Exception {
		slots = new HashSet<>();
		slotsRetriever = () -> new HashSet<>(slots);
		bulkChecker = new PhysicalSlotRequestBulkChecker(slotsRetriever, clock);
	}

	@Test
	public void testCreateBulk() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(Collections.emptyList());

		assertThat(bulk.getUnfulfillableSince(), is(clock.relativeTimeMillis()));
	}

	@Test
	public void testBulkFulfilledOnCheck() {
		final PhysicalSlotRequest request = createPhysicalSlotRequest();
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Collections.singletonList(request));

		bulk.markRequestFulfilled(request.getSlotRequestId(), new AllocationID());

		assertThat(checkBulkTimeout(bulk), is(PhysicalSlotRequestBulkChecker.TimeoutCheckResult.FULFILLED));
	}

	@Test
	public void testBulkTimeoutOnCheck() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Arrays.asList(createPhysicalSlotRequest()));

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		assertThat(checkBulkTimeout(bulk), is(PhysicalSlotRequestBulkChecker.TimeoutCheckResult.TIMEOUT));
	}

	@Test
	public void testBulkPendingOnCheckIfFulfillable() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Collections.singletonList(createPhysicalSlotRequest()));

		final PhysicalSlot slot = addOneSlot();
		occupyPhysicalSlot(slot, false);

		assertThat(checkBulkTimeout(bulk), is(PhysicalSlotRequestBulkChecker.TimeoutCheckResult.PENDING));
	}

	@Test
	public void testBulkPendingOnCheckIfUnfulfillableButNotTimedOut() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Collections.singletonList(createPhysicalSlotRequest()));

		assertThat(checkBulkTimeout(bulk), is(PhysicalSlotRequestBulkChecker.TimeoutCheckResult.PENDING));
	}

	@Test
	public void testBulkFulfillable() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Collections.singletonList(createPhysicalSlotRequest()));

		addOneSlot();

		assertThat(isFulfillable(bulk), is(true));
	}

	@Test
	public void testBulkUnfulfillableWithInsufficientSlots() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Arrays.asList(createPhysicalSlotRequest(), createPhysicalSlotRequest()));

		addOneSlot();

		assertThat(isFulfillable(bulk), is(false));
	}

	@Test
	public void testBulkUnfulfillableWithSlotAlreadyAssignedToBulk() {
		final PhysicalSlotRequest request = createPhysicalSlotRequest();
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Arrays.asList(request, createPhysicalSlotRequest()));

		final PhysicalSlot slot = addOneSlot();

		bulk.markRequestFulfilled(request.getSlotRequestId(), slot.getAllocationId());

		assertThat(isFulfillable(bulk), is(false));
	}

	@Test
	public void testBulkUnfulfillableWithSlotOccupiedIndefinitely() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Arrays.asList(createPhysicalSlotRequest(), createPhysicalSlotRequest()));

		final PhysicalSlot slot1 = addOneSlot();
		addOneSlot();

		occupyPhysicalSlot(slot1, true);

		assertThat(isFulfillable(bulk), is(false));
	}

	@Test
	public void testBulkFulfillableWithSlotOccupiedTemporarily() {
		final PhysicalSlotRequestBulk bulk = bulkChecker.createPhysicalSlotRequestBulk(
			Arrays.asList(createPhysicalSlotRequest(), createPhysicalSlotRequest()));

		final PhysicalSlot slot1 = addOneSlot();
		addOneSlot();

		occupyPhysicalSlot(slot1, false);

		assertThat(isFulfillable(bulk), is(true));
	}

	private static PhysicalSlotRequest createPhysicalSlotRequest() {
		return new PhysicalSlotRequest(
			new SlotRequestId(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			true);
	}

	private PhysicalSlot addOneSlot() {
		final PhysicalSlot slot = createPhysicalSlot();
		slots.add(slot);

		return slot;
	}

	private PhysicalSlotRequestBulkChecker.TimeoutCheckResult checkBulkTimeout(final PhysicalSlotRequestBulk bulk) {
		return bulkChecker.checkPhysicalSlotRequestBulkTimeout(bulk, TIMEOUT);
	}

	private boolean isFulfillable(final PhysicalSlotRequestBulk bulk) {
		return PhysicalSlotRequestBulkChecker.isSlotRequestBulkFulfillable(bulk, slotsRetriever);
	}
}
