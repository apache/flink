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

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class SchedulerTest extends TestLogger {

	@Test
	public void testAddAndRemoveInstance() {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		Instance i1 = getRandomInstance(2);
		Instance i2 = getRandomInstance(2);
		Instance i3 = getRandomInstance(2);

		assertEquals(0, scheduler.getNumberOfAvailableInstances());
		assertEquals(0, scheduler.getNumberOfAvailableSlots());
		scheduler.newInstanceAvailable(i1);
		assertEquals(1, scheduler.getNumberOfAvailableInstances());
		assertEquals(2, scheduler.getNumberOfAvailableSlots());
		scheduler.newInstanceAvailable(i2);
		assertEquals(2, scheduler.getNumberOfAvailableInstances());
		assertEquals(4, scheduler.getNumberOfAvailableSlots());
		scheduler.newInstanceAvailable(i3);
		assertEquals(3, scheduler.getNumberOfAvailableInstances());
		assertEquals(6, scheduler.getNumberOfAvailableSlots());

		// cannot add available instance again
		try {
			scheduler.newInstanceAvailable(i2);
			fail("Scheduler accepted instance twice");
		}
		catch (IllegalArgumentException e) {
			// bueno!
		}

		// some instances die
		assertEquals(3, scheduler.getNumberOfAvailableInstances());
		assertEquals(6, scheduler.getNumberOfAvailableSlots());
		scheduler.instanceDied(i2);
		assertEquals(2, scheduler.getNumberOfAvailableInstances());
		assertEquals(4, scheduler.getNumberOfAvailableSlots());

		// try to add a dead instance
		try {
			scheduler.newInstanceAvailable(i2);
			fail("Scheduler accepted dead instance");
		}
		catch (IllegalArgumentException e) {
			// stimmt

		}

		scheduler.instanceDied(i1);
		assertEquals(1, scheduler.getNumberOfAvailableInstances());
		assertEquals(2, scheduler.getNumberOfAvailableSlots());
		scheduler.instanceDied(i3);
		assertEquals(0, scheduler.getNumberOfAvailableInstances());
		assertEquals(0, scheduler.getNumberOfAvailableSlots());

		assertFalse(i1.isAlive());
		assertFalse(i2.isAlive());
		assertFalse(i3.isAlive());
	}
}
