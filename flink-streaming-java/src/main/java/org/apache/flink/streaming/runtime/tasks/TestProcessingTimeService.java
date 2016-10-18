/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is a {@link ProcessingTimeService} used <b>strictly for testing</b> the
 * processing time functionality.
 * */
public class TestProcessingTimeService extends ProcessingTimeService {

	private volatile long currentTime = 0;

	private volatile boolean isTerminated;
	private volatile boolean isQuiesced;

	// sorts the timers by timestamp so that they are processed in the correct order.
	private final Map<Long, List<ScheduledTimerFuture>> registeredTasks = new TreeMap<>();

	
	public void setCurrentTime(long timestamp) throws Exception {
		this.currentTime = timestamp;

		if (!isQuiesced) {
			// decide which timers to fire and put them in a list
			// we do not fire them here to be able to accommodate timers
			// that register other timers.
	
			Iterator<Map.Entry<Long, List<ScheduledTimerFuture>>> it = registeredTasks.entrySet().iterator();
			List<Map.Entry<Long, List<ScheduledTimerFuture>>> toRun = new ArrayList<>();
			while (it.hasNext()) {
				Map.Entry<Long, List<ScheduledTimerFuture>> t = it.next();
				if (t.getKey() <= this.currentTime) {
					toRun.add(t);
					it.remove();
				}
			}
	
			// now do the actual firing.
			for (Map.Entry<Long, List<ScheduledTimerFuture>> tasks: toRun) {
				long now = tasks.getKey();
				for (ScheduledTimerFuture task: tasks.getValue()) {
					task.getProcessingTimeCallback().onProcessingTime(now);
				}
			}
		}
	}

	@Override
	public long getCurrentProcessingTime() {
		return currentTime;
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		if (isTerminated) {
			throw new IllegalStateException("terminated");
		}
		if (isQuiesced) {
			return new ScheduledTimerFuture(null, -1);
		}

		if (timestamp <= currentTime) {
			try {
				target.onProcessingTime(timestamp);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		ScheduledTimerFuture result = new ScheduledTimerFuture(target, timestamp);

		List<ScheduledTimerFuture> tasks = registeredTasks.get(timestamp);
		if (tasks == null) {
			tasks = new ArrayList<>();
			registeredTasks.put(timestamp, tasks);
		}
		tasks.add(result);

		return result;
	}

	@Override
	public boolean isTerminated() {
		return isTerminated;
	}

	@Override
	public void quiesceAndAwaitPending() {
		if (!isTerminated) {
			isQuiesced = true;
			registeredTasks.clear();
		}
	}

	@Override
	public void shutdownService() {
		this.isTerminated = true;
	}

	public int getNumRegisteredTimers() {
		int count = 0;
		for (List<ScheduledTimerFuture> tasks: registeredTasks.values()) {
			count += tasks.size();
		}
		return count;
	}

	public Set<Long> getRegisteredTimerTimestamps() {
		Set<Long> actualTimestamps = new HashSet<>();
		for (List<ScheduledTimerFuture> timerFutures : registeredTasks.values()) {
			for (ScheduledTimerFuture timer : timerFutures) {
				actualTimestamps.add(timer.getTimestamp());
			}
		}
		return actualTimestamps;
	}

	// ------------------------------------------------------------------------

	private class ScheduledTimerFuture implements ScheduledFuture<Object> {

		private final ProcessingTimeCallback processingTimeCallback;

		private final long timestamp;

		public ScheduledTimerFuture(ProcessingTimeCallback processingTimeCallback, long timestamp) {
			this.processingTimeCallback = processingTimeCallback;
			this.timestamp = timestamp;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int compareTo(Delayed o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			List<ScheduledTimerFuture> scheduledTimerFutures = registeredTasks.get(timestamp);
			if (scheduledTimerFutures != null) {
				scheduledTimerFutures.remove(this);
			}
			return true;
		}

		@Override
		public boolean isCancelled() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isDone() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object get() throws InterruptedException, ExecutionException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			throw new UnsupportedOperationException();
		}

		public ProcessingTimeCallback getProcessingTimeCallback() {
			return processingTimeCallback;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}
}
