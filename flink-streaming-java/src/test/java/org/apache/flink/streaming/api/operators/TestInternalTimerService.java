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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.BiConsumerWithException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Implementation of {@link InternalTimerService} meant to use for testing.
 */
@Internal
public class TestInternalTimerService<K, N> implements InternalTimerService<N> {

	private long currentProcessingTime = Long.MIN_VALUE;

	private long currentWatermark = Long.MIN_VALUE;

	private final KeyContext keyContext;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final PriorityQueue<Timer<K, N>> processingTimeTimersQueue;
	private final Set<Timer<K, N>> processingTimeTimers;

	/**
	 * Current waiting watermark callbacks.
	 */
	private final Set<Timer<K, N>> watermarkTimers;
	private final PriorityQueue<Timer<K, N>> watermarkTimersQueue;

	public TestInternalTimerService(KeyContext keyContext) {
		this.keyContext = keyContext;

		watermarkTimers = new HashSet<>();
		watermarkTimersQueue = new PriorityQueue<>(100);
		processingTimeTimers = new HashSet<>();
		processingTimeTimersQueue = new PriorityQueue<>(100);
	}

	@Override
	public long currentProcessingTime() {
		return currentProcessingTime;
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		@SuppressWarnings("unchecked")
		Timer<K, N> timer = new Timer<>(time, (K) keyContext.getCurrentKey(), namespace);
		// make sure we only put one timer per key into the queue
		if (processingTimeTimers.add(timer)) {
			processingTimeTimersQueue.add(timer);
		}
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		@SuppressWarnings("unchecked")
		Timer<K, N> timer = new Timer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (watermarkTimers.add(timer)) {
			watermarkTimersQueue.add(timer);
		}
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		@SuppressWarnings("unchecked")
		Timer<K, N> timer = new Timer<>(time, (K) keyContext.getCurrentKey(), namespace);

		if (processingTimeTimers.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		@SuppressWarnings("unchecked")
		Timer<K, N> timer = new Timer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (watermarkTimers.remove(timer)) {
			watermarkTimersQueue.remove(timer);
		}
	}

	@Override
	public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) throws Exception {
		for (Timer<K, N> timer : watermarkTimers) {
			keyContext.setCurrentKey(timer.getKey());
			consumer.accept(timer.getNamespace(), timer.getTimestamp());
		}
	}

	@Override
	public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) throws Exception {
		for (Timer<K, N> timer : processingTimeTimers) {
			keyContext.setCurrentKey(timer.getKey());
			consumer.accept(timer.getNamespace(), timer.getTimestamp());
		}
	}

	public Collection<Timer<K, N>> advanceProcessingTime(long time) throws Exception {
		List<Timer<K, N>> result = new ArrayList<>();

		Timer<K, N> timer = processingTimeTimersQueue.peek();

		while (timer != null && timer.timestamp <= time) {
			processingTimeTimers.remove(timer);
			processingTimeTimersQueue.remove();
			result.add(timer);
			timer = processingTimeTimersQueue.peek();
		}

		currentProcessingTime = time;
		return result;
	}

	public Collection<Timer<K, N>> advanceWatermark(long time) throws Exception {
		List<Timer<K, N>> result = new ArrayList<>();

		Timer<K, N> timer = watermarkTimersQueue.peek();

		while (timer != null && timer.timestamp <= time) {
			watermarkTimers.remove(timer);
			watermarkTimersQueue.remove();
			result.add(timer);
			timer = watermarkTimersQueue.peek();
		}

		currentWatermark = time;
		return result;
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	public static class Timer<K, N> implements Comparable<Timer<K, N>> {
		private final long timestamp;
		private final K key;
		private final N namespace;

		public Timer(long timestamp, K key, N namespace) {
			this.timestamp = timestamp;
			this.key = key;
			this.namespace = namespace;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public K getKey() {
			return key;
		}

		public N getNamespace() {
			return namespace;
		}

		@Override
		public int compareTo(Timer<K, N> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
					&& key.equals(timer.key)
					&& namespace.equals(timer.namespace);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + namespace.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
					"timestamp=" + timestamp +
					", key=" + key +
					", namespace=" + namespace +
					'}';
		}
	}

	public int numProcessingTimeTimers() {
		return processingTimeTimers.size();
	}

	public int numEventTimeTimers() {
		return watermarkTimers.size();
	}

	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (Timer<K, N> timer : processingTimeTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (Timer<K, N> timer : watermarkTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

}
