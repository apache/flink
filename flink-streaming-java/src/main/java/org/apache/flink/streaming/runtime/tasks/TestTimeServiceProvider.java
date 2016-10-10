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

import org.apache.flink.streaming.runtime.operators.Triggerable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is a {@link TimeServiceProvider} used <b>strictly for testing</b> the
 * processing time functionality.
 * */
public class TestTimeServiceProvider extends TimeServiceProvider {

	private volatile long currentTime = 0;

	private volatile boolean isTerminated;
	private volatile boolean isQuiesced;

	// sorts the timers by timestamp so that they are processed in the correct order.
	private final Map<Long, List<Triggerable>> registeredTasks = new TreeMap<>();

	
	public void setCurrentTime(long timestamp) throws Exception {
		this.currentTime = timestamp;

		if (!isQuiesced) {
			// decide which timers to fire and put them in a list
			// we do not fire them here to be able to accommodate timers
			// that register other timers.
	
			Iterator<Map.Entry<Long, List<Triggerable>>> it = registeredTasks.entrySet().iterator();
			List<Map.Entry<Long, List<Triggerable>>> toRun = new ArrayList<>();
			while (it.hasNext()) {
				Map.Entry<Long, List<Triggerable>> t = it.next();
				if (t.getKey() <= this.currentTime) {
					toRun.add(t);
					it.remove();
				}
			}
	
			// now do the actual firing.
			for (Map.Entry<Long, List<Triggerable>> tasks: toRun) {
				long now = tasks.getKey();
				for (Triggerable task: tasks.getValue()) {
					task.trigger(now);
				}
			}
		}
	}

	@Override
	public long getCurrentProcessingTime() {
		return currentTime;
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, Triggerable target) {
		if (isTerminated) {
			throw new IllegalStateException("terminated");
		}
		if (isQuiesced) {
			return new DummyFuture();
		}

		if (timestamp <= currentTime) {
			try {
				target.trigger(timestamp);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		List<Triggerable> tasks = registeredTasks.get(timestamp);
		if (tasks == null) {
			tasks = new ArrayList<>();
			registeredTasks.put(timestamp, tasks);
		}
		tasks.add(target);

		return new DummyFuture();
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
		for (List<Triggerable> tasks: registeredTasks.values()) {
			count += tasks.size();
		}
		return count;
	}

	// ------------------------------------------------------------------------

	private static class DummyFuture implements ScheduledFuture<Object> {

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
	}
}
