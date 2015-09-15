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

package org.apache.flink.streaming.runtime.operators;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class TriggerTimerTest {
	
	@Test
	public void testThreadGroupAndShutdown() {
		try {
			TriggerTimer timer = new TriggerTimer();
			
			// first one spawns thread
			timer.scheduleTriggerAt(new Triggerable() {
				@Override
				public void trigger(long timestamp) {}
			}, System.currentTimeMillis());
			
			assertEquals(1, TriggerTimer.TRIGGER_THREADS_GROUP.activeCount());
			
			// thread needs to die in time
			timer.shutdown();
			
			long deadline = System.currentTimeMillis() + 2000;
			while (TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}

			assertEquals("Trigger timer thread did not properly shut down",
					0, TriggerTimer.TRIGGER_THREADS_GROUP.activeCount());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void checkScheduledTimestampe() {
		try {
			final TriggerTimer timer = new TriggerTimer();

			final AtomicReference<Throwable> errorRef = new AtomicReference<>();
			
			final long t1 = System.currentTimeMillis();
			final long t2 = System.currentTimeMillis() - 200;
			final long t3 = System.currentTimeMillis() + 100;
			final long t4 = System.currentTimeMillis() + 200;
			
			timer.scheduleTriggerAt(new ValidatingTriggerable(errorRef, t1, 0), t1);
			timer.scheduleTriggerAt(new ValidatingTriggerable(errorRef, t2, 1), t2);
			timer.scheduleTriggerAt(new ValidatingTriggerable(errorRef, t3, 2), t3);
			timer.scheduleTriggerAt(new ValidatingTriggerable(errorRef, t4, 3), t4);
			
			long deadline = System.currentTimeMillis() + 5000;
			while (errorRef.get() == null &&
					ValidatingTriggerable.numInSequence < 4 &&
					System.currentTimeMillis() < deadline)
			{
				Thread.sleep(100);
			}
			
			// handle errors
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
				fail(errorRef.get().getMessage());
			}
			
			assertEquals(4, ValidatingTriggerable.numInSequence);
			
			timer.shutdown();
			
			// wait until the trigger thread is shut down. otherwise, the other tests may become unstable
			deadline = System.currentTimeMillis() + 2000;
			while (TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}

			assertEquals("Trigger timer thread did not properly shut down", 
					0, TriggerTimer.TRIGGER_THREADS_GROUP.activeCount());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	private static class ValidatingTriggerable implements Triggerable {
		
		static int numInSequence;
		
		private final AtomicReference<Throwable> errorRef;
		
		private final long expectedTimestamp;
		private final int expectedInSequence;

		private ValidatingTriggerable(AtomicReference<Throwable> errorRef, long expectedTimestamp, int expectedInSequence) {
			this.errorRef = errorRef;
			this.expectedTimestamp = expectedTimestamp;
			this.expectedInSequence = expectedInSequence;
		}

		@Override
		public void trigger(long timestamp) {
			try {
				assertEquals(expectedTimestamp, timestamp);
				assertEquals(expectedInSequence, numInSequence);
				numInSequence++;
			}
			catch (Throwable t) {
				errorRef.compareAndSet(null, t);
			}
		}
	}
}
