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

import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A timer that triggers targets at a specific point in the future. This timer executes single-threaded,
 * which means that never more than one trigger will be executed at the same time.
 * <p>
 * This timer generally maintains order of trigger events. This means that for two triggers scheduled at
 * different times, the one scheduled for the later time will be executed after the one scheduled for the
 * earlier time.
 */
public class TriggerTimer {
	
	/** The thread group that holds all trigger timer threads */
	public static final ThreadGroup TRIGGER_THREADS_GROUP = new ThreadGroup("Triggers");
	
	/** The executor service that */
	private final ScheduledExecutorService scheduler;


	/**
	 * Creates a new trigger timer, where the timer thread has the default name "TriggerTimer Thread".
	 */
	public TriggerTimer() {
		this("TriggerTimer Thread");
	}

	/**
	 * Creates a new trigger timer, where the timer thread has the given name.
	 * 
	 * @param triggerName The name for the trigger thread.
	 */
	public TriggerTimer(String triggerName) {
		this.scheduler = Executors.newSingleThreadScheduledExecutor(
				new DispatcherThreadFactory(TRIGGER_THREADS_GROUP, triggerName));
	}

	/**
	 * Schedules a new trigger event. The trigger event will occur roughly at the given timestamp.
	 * If the timestamp is in the past (or now), the trigger will be queued for immediate execution. Note that other
	 * triggers that are to be executed now will be executed before this trigger.
	 * 
	 * @param target The target to be triggered.
	 * @param timestamp The timestamp when the trigger should occur, and the timestamp given
	 *                  to the trigger-able target.
	 */
	public void scheduleTriggerAt(Triggerable target, long timestamp) {
		long delay = Math.max(timestamp - System.currentTimeMillis(), 0);
		
		scheduler.schedule(
				new TriggerTask(target, timestamp),
				delay,
				TimeUnit.MILLISECONDS);
	}

	/**
	 * Shuts down the trigger timer, canceling all pending triggers and stopping the trigger thread.
	 */
	public void shutdown() {
		scheduler.shutdownNow();
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 * <p>
	 * This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	@SuppressWarnings("FinalizeDoesntCallSuperFinalize")
	protected void finalize() {
		shutdown();
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Internal task that is invoked by the scheduled executor and triggers the target.
	 */
	private static final class TriggerTask implements Runnable {
		
		private final Triggerable target;
		private final long timestamp;

		TriggerTask(Triggerable target, long timestamp) {
			this.target = target;
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			target.trigger(timestamp);
		}
	}
}
