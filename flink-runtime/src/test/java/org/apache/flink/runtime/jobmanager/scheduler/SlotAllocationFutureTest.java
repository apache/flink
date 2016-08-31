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
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SlotAllocationFutureTest {

	@Test
	public void testInvalidActions() {
		try {
			final SlotAllocationFuture future = new SlotAllocationFuture();
			
			SlotAllocationFutureAction action = new SlotAllocationFutureAction() {
				@Override
				public void slotAllocated(SimpleSlot slot) {}
			};
			
			future.setFutureAction(action);
			try {
				future.setFutureAction(action);
				fail();
			} catch (IllegalStateException e) {
				// expected
			}

			final Instance instance1 = SchedulerTestUtils.getRandomInstance(1);
			final Instance instance2 = SchedulerTestUtils.getRandomInstance(1);

			final SimpleSlot slot1 = new SimpleSlot(new JobID(), instance1,
					instance1.getTaskManagerLocation(), 0, instance1.getActorGateway(), null, null);
			final SimpleSlot slot2 = new SimpleSlot(new JobID(), instance2,
					instance2.getTaskManagerLocation(), 0, instance2.getActorGateway(), null, null);
			
			future.setSlot(slot1);
			try {
				future.setSlot(slot2);
				fail();
			} catch (IllegalStateException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void setWithAction() {
		try {
			
			// action before the slot
			{
				final AtomicInteger invocations = new AtomicInteger();

				final Instance instance = SchedulerTestUtils.getRandomInstance(1);

				final SimpleSlot thisSlot = new SimpleSlot(new JobID(), instance,
						instance.getTaskManagerLocation(), 0, instance.getActorGateway(), null, null);
				
				SlotAllocationFuture future = new SlotAllocationFuture();
				
				future.setFutureAction(new SlotAllocationFutureAction() {
					@Override
					public void slotAllocated(SimpleSlot slot) {
						assertEquals(thisSlot, slot);
						invocations.incrementAndGet();
					}
				});
				
				future.setSlot(thisSlot);
				
				assertEquals(1, invocations.get());
			}
			
			// slot before action
			{
				final AtomicInteger invocations = new AtomicInteger();
				final Instance instance = SchedulerTestUtils.getRandomInstance(1);
				
				final SimpleSlot thisSlot = new SimpleSlot(new JobID(), instance,
						instance.getTaskManagerLocation(), 0, instance.getActorGateway(), null, null);
				
				SlotAllocationFuture future = new SlotAllocationFuture();
				future.setSlot(thisSlot);
				
				future.setFutureAction(new SlotAllocationFutureAction() {
					@Override
					public void slotAllocated(SimpleSlot slot) {
						assertEquals(thisSlot, slot);
						invocations.incrementAndGet();
					}
				});
				
				assertEquals(1, invocations.get());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void setSync() {
		try {
			// sync before setting the slot
			{
				final AtomicInteger invocations = new AtomicInteger();
				final AtomicBoolean error = new AtomicBoolean();

				final Instance instance = SchedulerTestUtils.getRandomInstance(1);

				final SimpleSlot thisSlot = new SimpleSlot(new JobID(), instance,
						instance.getTaskManagerLocation(), 0, instance.getActorGateway(), null, null);
				
				final SlotAllocationFuture future = new SlotAllocationFuture();
				
				
				Runnable r = new Runnable() {
					@Override
					public void run() {
						try {
							SimpleSlot syncSlot = future.waitTillCompleted();
							if (syncSlot == null || syncSlot != thisSlot) {
								error.set(true);
								return;
							}
							invocations.incrementAndGet();
						}
						catch (Throwable t) {
							error.set(true);
						}
					}
				};
				
				Thread syncer = new Thread(r);
				syncer.start();
				
				// wait, and give the sync thread a chance to sync
				Thread.sleep(10);
				future.setSlot(thisSlot);
				
				syncer.join();
				
				assertFalse(error.get());
				assertEquals(1, invocations.get());
			}
			
			// setting slot before syncing
			{
				final Instance instance = SchedulerTestUtils.getRandomInstance(1);

				final SimpleSlot thisSlot = new SimpleSlot(new JobID(), instance, 
						instance.getTaskManagerLocation(), 0, instance.getActorGateway(), null, null);
				final SlotAllocationFuture future = new SlotAllocationFuture();

				future.setSlot(thisSlot);
				
				SimpleSlot retrieved = future.waitTillCompleted();
				
				assertNotNull(retrieved);
				assertEquals(thisSlot, retrieved);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
