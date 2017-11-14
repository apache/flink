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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.LogicalSlot;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.areAllDistinct;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getDummyTask;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
			LogicalSlot s1 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			LogicalSlot s2 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			LogicalSlot s3 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			LogicalSlot s4 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			LogicalSlot s5 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			
			// the slots should all be different
			assertTrue(areAllDistinct(s1, s2, s3, s4, s5));
			
			try {
				scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
				fail("Scheduler accepted scheduling request without available resource.");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			
			// release some slots again
			s3.releaseSlot();
			s4.releaseSlot();
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			// now we can schedule some more slots
			LogicalSlot s6 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			LogicalSlot s7 = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
			
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
			List<CompletableFuture<LogicalSlot>> allAllocatedSlots = new ArrayList<>();

			// slots that need to be released
			final Set<LogicalSlot> toRelease = new HashSet<>();

			// flag to track errors in the concurrent thread
			final AtomicBoolean errored = new AtomicBoolean(false);

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

								Iterator<LogicalSlot> iter = toRelease.iterator();
								LogicalSlot next = iter.next();
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
				CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), true, Collections.emptyList());
				future.thenAcceptAsync(
					(LogicalSlot slot) -> {
						synchronized (toRelease) {
							toRelease.add(slot);
							toRelease.notifyAll();
						}
					},
					TestingUtils.defaultExecutionContext());
				allAllocatedSlots.add(future);
			}

			disposeThread.join();

			assertFalse("The slot releasing thread caused an error.", errored.get());

			List<LogicalSlot> slotsAfter = new ArrayList<>();
			for (CompletableFuture<LogicalSlot> future : allAllocatedSlots) {
				slotsAfter.add(future.get());
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
			
			List<LogicalSlot> slots = new ArrayList<>();
			slots.add(scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get());
			slots.add(scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get());
			slots.add(scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get());
			slots.add(scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get());
			slots.add(scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get());
			
			i2.markDead();
			
			for (LogicalSlot slot : slots) {
				if (Objects.equals(slot.getTaskManagerLocation().getResourceID(), i2.getTaskManagerID())) {
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
				scheduler.allocateSlot(new ScheduledUnit(getDummyTask()), false, Collections.emptyList()).get();
				fail("Scheduler served a slot from a dead instance");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
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
			LogicalSlot s1 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(new Instance[0])), false, Collections.emptyList()).get();
			
			// figure out how we use the location hints
			ResourceID firstResourceId = s1.getTaskManagerLocation().getResourceID();

			List<Instance> instances = Arrays.asList(i1, i2, i3);

			int index = 0;
			for (; index < instances.size(); index++) {
				if (Objects.equals(instances.get(index).getTaskManagerID(), firstResourceId)) {
					break;
				}
			}

			Instance first = instances.get(index);
			Instance second = instances.get((index + 1) % instances.size());
			Instance third = instances.get((index + 2) % instances.size());

			// something that needs to go to the first instance again
			LogicalSlot s2 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(s1.getTaskManagerLocation())), false, Collections.singleton(s1.getTaskManagerLocation())).get();
			assertEquals(first.getTaskManagerID(), s2.getTaskManagerLocation().getResourceID());

			// first or second --> second, because first is full
			LogicalSlot s3 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(first, second)), false, Arrays.asList(first.getTaskManagerLocation(), second.getTaskManagerLocation())).get();
			assertEquals(second.getTaskManagerID(), s3.getTaskManagerLocation().getResourceID());
			
			// first or third --> third (because first is full)
			LogicalSlot s4 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(first, third)), false, Arrays.asList(first.getTaskManagerLocation(), third.getTaskManagerLocation())).get();
			LogicalSlot s5 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(first, third)), false, Arrays.asList(first.getTaskManagerLocation(), third.getTaskManagerLocation())).get();
			assertEquals(third.getTaskManagerID(), s4.getTaskManagerLocation().getResourceID());
			assertEquals(third.getTaskManagerID(), s5.getTaskManagerLocation().getResourceID());
			
			// first or third --> second, because all others are full
			LogicalSlot s6 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(first, third)), false, Arrays.asList(first.getTaskManagerLocation(), third.getTaskManagerLocation())).get();
			assertEquals(second.getTaskManagerID(), s6.getTaskManagerLocation().getResourceID());
			
			// release something on the first and second instance
			s2.releaseSlot();
			s6.releaseSlot();
			
			LogicalSlot s7 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(first, third)), false, Arrays.asList(first.getTaskManagerLocation(), third.getTaskManagerLocation())).get();
			assertEquals(first.getTaskManagerID(), s7.getTaskManagerLocation().getResourceID());
			
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
