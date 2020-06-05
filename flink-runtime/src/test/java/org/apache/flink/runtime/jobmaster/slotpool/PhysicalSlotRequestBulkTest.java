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

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link PhysicalSlotRequestBulk}.
 */
public class PhysicalSlotRequestBulkTest extends TestLogger {

	private final ManualClock clock = new ManualClock();

	@Test
	public void testMarkBulkUnfulfillable() {
		final PhysicalSlotRequestBulk bulk = new PhysicalSlotRequestBulk(Collections.emptyList());

		clock.advanceTime(456, TimeUnit.MILLISECONDS);
		bulk.markUnfulfillable(clock.relativeTimeMillis());

		assertThat(bulk.getUnfulfillableSince(), is(clock.relativeTimeMillis()));
	}

	@Test
	public void testUnfulfillableTimestampWillNotBeOverriddenByFollowingUnfulfillableTimestamp() {
		final PhysicalSlotRequestBulk bulk = new PhysicalSlotRequestBulk(Collections.emptyList());

		final long unfulfillableSince = clock.relativeTimeMillis();
		bulk.markUnfulfillable(unfulfillableSince);

		clock.advanceTime(456, TimeUnit.MILLISECONDS);
		bulk.markUnfulfillable(clock.relativeTimeMillis());

		assertThat(bulk.getUnfulfillableSince(), is(unfulfillableSince));
	}
}
