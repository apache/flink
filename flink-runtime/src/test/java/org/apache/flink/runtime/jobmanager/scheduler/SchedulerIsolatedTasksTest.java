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

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.areAllDistinct;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getDummyTask;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertex;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.junit.Assert.*;

import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.runtime.instance.Instance;

/**
 * Tests for the {@link Scheduler} when scheduling individual tasks.
 */
public class SchedulerIsolatedTasksTest {

	@Test
	public void testAddAndRemoveInstance() {
		try {
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
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleImmediately() {
		try {
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			assertEquals(0, scheduler.getNumberOfAvailableSlots());
			
			scheduler.newInstanceAvailable(getRandomInstance(2));
			scheduler.newInstanceAvailable(getRandomInstance(1));
			scheduler.newInstanceAvailable(getRandomInstance(2));
			assertEquals(5, scheduler.getNumberOfAvailableSlots());
			
			// schedule something into all slots
			SimpleSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			SimpleSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			SimpleSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			SimpleSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			SimpleSlot s5 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			
			// the slots should all be different
			assertTrue(areAllDistinct(s1, s2, s3, s4, s5));
			
			try {
				scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
				fail("Scheduler accepted scheduling request without available resource.");
			}
			catch (NoResourceAvailableException e) {
				// pass!
			}
			
			// release some slots again
			s3.releaseSlot();
			s4.releaseSlot();
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			// now we can schedule some more slots
			SimpleSlot s6 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			SimpleSlot s7 = scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
			
			assertTrue(areAllDistinct(s1, s2, s3, s4, s5, s6, s7));
			
			// release all
			
			s1.releaseSlot();
			s2.releaseSlot();
			s5.releaseSlot();
			s6.releaseSlot();
			s7.releaseSlot();
			
			assertEquals(5, scheduler.getNumberOfAvailableSlots());
			
			// check that slots that are released twice (accidentally) do not mess things up
			
			s1.releaseSlot();
			s2.releaseSlot();
			s5.releaseSlot();
			s6.releaseSlot();
			s7.releaseSlot();
			
			assertEquals(5, scheduler.getNumberOfAvailableSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleQueueing() {
		final int NUM_INSTANCES = 50;
		final int NUM_SLOTS_PER_INSTANCE = 3;
		final int NUM_TASKS_TO_SCHEDULE = 2000;

		try {
			// note: since this test asynchronously releases slots, the executor needs release workers.
			// doing the release call synchronous can lead to a deadlock
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

			for (int i = 0; i < NUM_INSTANCES; i++) {
				scheduler.newInstanceAvailable(getRandomInstance((int) (Math.random() * NUM_SLOTS_PER_INSTANCE) + 1));
			}

			assertEquals(NUM_INSTANCES, scheduler.getNumberOfAvailableInstances());
			final int totalSlots = scheduler.getNumberOfAvailableSlots();

			// all slots we ever got.
			List<SlotAllocationFuture> allAllocatedSlots = new ArrayList<SlotAllocationFuture>();

			// slots that need to be released
			final Set<SimpleSlot> toRelease = new HashSet<SimpleSlot>();

			// flag to track errors in the concurrent thread
			final AtomicBoolean errored = new AtomicBoolean(false);


			SlotAllocationFutureAction action = new SlotAllocationFutureAction() {
				@Override
				public void slotAllocated(SimpleSlot slot) {
					synchronized (toRelease) {
						toRelease.add(slot);
						toRelease.notifyAll();
					}
				}
			};

			// thread to asynchronously release slots
			Runnable disposer = new Runnable() {

				@Override
				public void run() {
					try {
						int recycled = 0;
						while (recycled < NUM_TASKS_TO_SCHEDULE) {
							synchronized (toRelease) {
								while (toRelease.isEmpty()) {
									toRelease.wait();
								}

								Iterator<SimpleSlot> iter = toRelease.iterator();
								SimpleSlot next = iter.next();
								iter.remove();

								next.releaseSlot();
								recycled++;
							}
						}
					} catch (Throwable t) {
						errored.set(true);
					}
				}
			};

			Thread disposeThread = new Thread(disposer);
			disposeThread.start();

			for (int i = 0; i < NUM_TASKS_TO_SCHEDULE; i++) {
				SlotAllocationFuture future = scheduler.scheduleQueued(new ScheduledUnit(getDummyTask()));
				future.setFutureAction(action);
				allAllocatedSlots.add(future);
			}

			disposeThread.join();

			assertFalse("The slot releasing thread caused an error.", errored.get());

			List<SimpleSlot> slotsAfter = new ArrayList<SimpleSlot>();
			for (SlotAllocationFuture future : allAllocatedSlots) {
				slotsAfter.add(future.waitTillAllocated());
			}

			assertEquals("All instances should have available slots.", NUM_INSTANCES,
					scheduler.getNumberOfInstancesWithAvailableSlots());

			// the slots should all be different
			assertTrue(areAllDistinct(slotsAfter.toArray()));

			assertEquals("All slots should be available.", totalSlots,
					scheduler.getNumberOfAvailableSlots());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleWithDyingInstances() {
		try {
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			
			Instance i1 = getRandomInstance(2);
			Instance i2 = getRandomInstance(2);
			Instance i3 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i3);
			
			List<SimpleSlot> slots = new ArrayList<SimpleSlot>();
			slots.add(scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask())));
			slots.add(scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask())));
			slots.add(scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask())));
			slots.add(scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask())));
			slots.add(scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask())));
			
			i2.markDead();
			
			for (SimpleSlot slot : slots) {
				if (slot.getInstance() == i2) {
					assertTrue(slot.isCanceled());
				} else {
					assertFalse(slot.isCanceled());
				}
				
				slot.releaseSlot();
			}
			
			assertEquals(3, scheduler.getNumberOfAvailableSlots());
			
			i1.markDead();
			i3.markDead();
			
			// cannot get another slot, since all instances are dead
			try {
				scheduler.scheduleImmediately(new ScheduledUnit(getDummyTask()));
				fail("Scheduler served a slot from a dead instance");
			}
			catch (NoResourceAvailableException e) {
				// fine
			}
			catch (Exception e) {
				fail("Wrong exception type.");
			}
			
			// now the latest, the scheduler should have noticed (through the lazy mechanisms)
			// that all instances have vanished
			assertEquals(0, scheduler.getNumberOfInstancesWithAvailableSlots());
			assertEquals(0, scheduler.getNumberOfAvailableSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSchedulingLocation() {
		try {
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
			
			Instance i1 = getRandomInstance(2);
			Instance i2 = getRandomInstance(2);
			Instance i3 = getRandomInstance(2);
			
			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i3);
			
			// schedule something on an arbitrary instance
			SimpleSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Collections.<Instance>emptyList())));
			
			// figure out how we use the location hints
			Instance first = s1.getInstance();
			Instance second = first != i1 ? i1 : i2;
			Instance third = first == i3 ? i2 : i3;
			
			// something that needs to go to the first instance again
			SimpleSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Collections.singletonList(s1.getInstance()))));
			assertEquals(first, s2.getInstance());

			// first or second --> second, because first is full
			SimpleSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Arrays.asList(first, second))));
			assertEquals(second, s3.getInstance());
			
			// first or third --> third (because first is full)
			SimpleSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Arrays.asList(first, third))));
			SimpleSlot s5 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Arrays.asList(first, third))));
			assertEquals(third, s4.getInstance());
			assertEquals(third, s5.getInstance());
			
			// first or third --> second, because all others are full
			SimpleSlot s6 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Arrays.asList(first, third))));
			assertEquals(second, s6.getInstance());
			
			// release something on the first and second instance
			s2.releaseSlot();
			s6.releaseSlot();
			
			SimpleSlot s7 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(Arrays.asList(first, third))));
			assertEquals(first, s7.getInstance());
			
			assertEquals(1, scheduler.getNumberOfUnconstrainedAssignments());
			assertEquals(1, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(5, scheduler.getNumberOfLocalizedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
