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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.runtime.operators.TestProcessingTimeServiceTest.ReferenceSettingExceptionHandler;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SystemProcessingTimeServiceTest {

	@Test
	public void testTriggerHoldsLock() throws Exception {

		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = new SystemProcessingTimeService(
				new ReferenceSettingExceptionHandler(errorRef), lock);

		try {
			assertEquals(0, timer.getNumTasksScheduled());

			// schedule something
			ScheduledFuture<?> future = timer.registerTimer(System.currentTimeMillis(), new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) {
					assertTrue(Thread.holdsLock(lock));
				}
			});

			// wait until the execution is over
			future.get();
			assertEquals(0, timer.getNumTasksScheduled());

			// check that no asynchronous error was reported
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testImmediateShutdown() throws Exception {

		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = new SystemProcessingTimeService(
				new ReferenceSettingExceptionHandler(errorRef), lock);

		try {
			assertFalse(timer.isTerminated());

			final OneShotLatch latch = new OneShotLatch();

			// the task should trigger immediately and should block until terminated with interruption
			timer.registerTimer(System.currentTimeMillis(), new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					latch.trigger();
					Thread.sleep(100000000);
				}
			});

			latch.await();
			timer.shutdownService();

			// can only enter this scope after the triggerable is interrupted
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (lock) {
				assertTrue(timer.isTerminated());
			}

			try {
				timer.registerTimer(System.currentTimeMillis() + 1000, new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) {}
				});

				fail("should result in an exception");
			}
			catch (IllegalStateException e) {
				// expected
			}

			// obviously, we have an asynchronous interrupted exception
			assertNotNull(errorRef.get());
			assertTrue(errorRef.get().getCause() instanceof InterruptedException);

			assertEquals(0, timer.getNumTasksScheduled());
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testQuiescing() throws Exception {

		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = new SystemProcessingTimeService(
				new ReferenceSettingExceptionHandler(errorRef), lock);

		try {
			final OneShotLatch latch = new OneShotLatch();

			final ReentrantLock scopeLock = new ReentrantLock();

			timer.registerTimer(System.currentTimeMillis() + 20, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					scopeLock.lock();
					try {
						latch.trigger();
						// delay a bit before leaving the method
						Thread.sleep(5);
					} finally {
						scopeLock.unlock();
					}
				}
			});

			// after the task triggered, shut the timer down cleanly, waiting for the task to finish
			latch.await();
			timer.quiesceAndAwaitPending();

			// should be able to immediately acquire the lock, since the task must have exited by now 
			assertTrue(scopeLock.tryLock());

			// should be able to schedule more tasks (that never get executed)
			ScheduledFuture<?> future = timer.registerTimer(System.currentTimeMillis() - 5, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					throw new Exception("test");
				}
			});
			assertNotNull(future);

			// nothing should be scheduled right now
			assertEquals(0, timer.getNumTasksScheduled());

			// check that no asynchronous error was reported - that ensures that the newly scheduled 
			// triggerable did, in fact, not trigger
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testFutureCancellation() throws Exception {

		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = new SystemProcessingTimeService(
				new ReferenceSettingExceptionHandler(errorRef), lock);

		try {
			assertEquals(0, timer.getNumTasksScheduled());

			// schedule something
			ScheduledFuture<?> future = timer.registerTimer(System.currentTimeMillis() + 100000000, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) {}
			});
			assertEquals(1, timer.getNumTasksScheduled());

			future.cancel(false);

			assertEquals(0, timer.getNumTasksScheduled());

			// check that no asynchronous error was reported
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}

	@Test
	public void testExceptionReporting() throws InterruptedException {
		final AtomicBoolean exceptionWasThrown = new AtomicBoolean(false);
		final OneShotLatch latch = new OneShotLatch();
		final Object lock = new Object();

		ProcessingTimeService timeServiceProvider = new SystemProcessingTimeService(
				new AsyncExceptionHandler() {
					@Override
					public void handleAsyncException(String message, Throwable exception) {
						exceptionWasThrown.set(true);
						latch.trigger();
					}
				}, lock);
		
		timeServiceProvider.registerTimer(System.currentTimeMillis(), new ProcessingTimeCallback() {
			@Override
			public void onProcessingTime(long timestamp) throws Exception {
				throw new Exception("Exception in Timer");
			}
		});

		latch.await();
		assertTrue(exceptionWasThrown.get());
	}

	@Test
	public void testTimerSorting() throws Exception {
		final Object lock = new Object();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		final SystemProcessingTimeService timer = new SystemProcessingTimeService(
				new ReferenceSettingExceptionHandler(errorRef), lock);

		try {
			final OneShotLatch sync = new OneShotLatch();

			// we block the timer execution to make sure we have all the time
			// to register some additional timers out of order
			timer.registerTimer(System.currentTimeMillis(), new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					sync.await();
				}
			});
			
			// schedule two timers out of order something
			final long now = System.currentTimeMillis();
			final long time1 = now + 6;
			final long time2 = now + 5;
			final long time3 = now + 8;
			final long time4 = now - 2;

			final ArrayBlockingQueue<Long> timestamps = new ArrayBlockingQueue<>(4);
			ProcessingTimeCallback trigger = new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) {
					timestamps.add(timestamp);
				}
			};

			// schedule
			ScheduledFuture<?> future1 = timer.registerTimer(time1, trigger);
			ScheduledFuture<?> future2 = timer.registerTimer(time2, trigger);
			ScheduledFuture<?> future3 = timer.registerTimer(time3, trigger);
			ScheduledFuture<?> future4 = timer.registerTimer(time4, trigger);

			// now that everything is scheduled, unblock the timer service
			sync.trigger();

			// wait until both are complete
			future1.get();
			future2.get();
			future3.get();
			future4.get();

			// verify that the order is 4 - 2 - 1 - 3
			assertEquals(4, timestamps.size());
			assertEquals(time4, timestamps.take().longValue());
			assertEquals(time2, timestamps.take().longValue());
			assertEquals(time1, timestamps.take().longValue());
			assertEquals(time3, timestamps.take().longValue());

			// check that no asynchronous error was reported
			if (errorRef.get() != null) {
				throw new Exception(errorRef.get());
			}
		}
		finally {
			timer.shutdownService();
		}
	}
}
