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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 */
public class HeapInternalTimerService<K, N> implements InternalTimerService<N>, Triggerable {

	private final TypeSerializer<K> keySerializer;

	private final TypeSerializer<N> namespaceSerializer;

	private final ProcessingTimeService processingTimeService;

	private long currentWatermark = Long.MIN_VALUE;

	private final org.apache.flink.streaming.api.operators.Triggerable<K, N> triggerTarget;

	private final KeyContext keyContext;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final PriorityQueue<InternalTimer<K, N>> processingTimeTimersQueue;
	private final Set<InternalTimer<K, N>> processingTimeTimers;

	protected ScheduledFuture<?> nextTimer = null;

	/**
	 * Currently waiting watermark callbacks.
	 */
	private final Set<InternalTimer<K, N>> watermarkTimers;
	private final PriorityQueue<InternalTimer<K, N>> watermarkTimersQueue;

	public HeapInternalTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			org.apache.flink.streaming.api.operators.Triggerable<K, N> triggerTarget,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {
		this.keySerializer = checkNotNull(keySerializer);
		this.namespaceSerializer = checkNotNull(namespaceSerializer);
		this.triggerTarget = checkNotNull(triggerTarget);
		this.keyContext = keyContext;
		this.processingTimeService = checkNotNull(processingTimeService);

		watermarkTimers = new HashSet<>();
		watermarkTimersQueue = new PriorityQueue<>(100);

		processingTimeTimers = new HashSet<>();
		processingTimeTimersQueue = new PriorityQueue<>(100);
	}

	public HeapInternalTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			org.apache.flink.streaming.api.operators.Triggerable<K, N> triggerTarget,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService,
			RestoredTimers<K, N> restoredTimers) {

		this.keySerializer = checkNotNull(keySerializer);
		this.namespaceSerializer = checkNotNull(namespaceSerializer);
		this.triggerTarget = checkNotNull(triggerTarget);
		this.keyContext = keyContext;
		this.processingTimeService = checkNotNull(processingTimeService);

		watermarkTimers = restoredTimers.watermarkTimers;
		watermarkTimersQueue = restoredTimers.watermarkTimersQueue;

		processingTimeTimers = restoredTimers.processingTimeTimers;
		processingTimeTimersQueue = restoredTimers.processingTimeTimersQueue;

		// re-register the restored timers (if any)
		if (processingTimeTimersQueue.size() > 0) {
			nextTimer =
					processingTimeService.registerTimer(processingTimeTimersQueue.peek().getTimestamp(), this);
		}
	}


	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);

		// make sure we only put one timer per key into the queue
		if (processingTimeTimers.add(timer)) {

			InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;

			processingTimeTimersQueue.add(timer);

			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				nextTimer = processingTimeService.registerTimer(time, this);
			}
		}
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (watermarkTimers.add(timer)) {
			watermarkTimersQueue.add(timer);
		}
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);

		if (processingTimeTimers.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (watermarkTimers.remove(timer)) {
			watermarkTimersQueue.remove(timer);
		}
	}

	@Override
	public void trigger(long time) throws Exception {

		InternalTimer<K, N> timer;

		while ((timer  = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			timer = processingTimeTimersQueue.peek();

			processingTimeTimers.remove(timer);
			processingTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
		}
	}

	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer = watermarkTimersQueue.peek();

		while (timer != null && timer.getTimestamp() <= time) {
			watermarkTimers.remove(timer);
			watermarkTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);

			timer = watermarkTimersQueue.peek();
		}
	}

	public void snapshotTimers(OutputStream outStream) throws IOException {
		InstantiationUtil.serializeObject(outStream, keySerializer);
		InstantiationUtil.serializeObject(outStream, namespaceSerializer);

		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(outStream);

		out.writeInt(watermarkTimersQueue.size());
		for (InternalTimer<K, N> timer : watermarkTimersQueue) {
			keySerializer.serialize(timer.getKey(), out);
			namespaceSerializer.serialize(timer.getNamespace(), out);
			out.writeLong(timer.getTimestamp());
		}

		out.writeInt(processingTimeTimers.size());
		for (InternalTimer<K, N> timer : processingTimeTimers) {
			keySerializer.serialize(timer.getKey(), out);
			namespaceSerializer.serialize(timer.getNamespace(), out);
			out.writeLong(timer.getTimestamp());
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
		for (InternalTimer<K, N> timer : processingTimeTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : watermarkTimers) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}

		return count;
	}

	public static class RestoredTimers<K, N> {

		private final TypeSerializer<K> keySerializer;

		private final TypeSerializer<N> namespaceSerializer;

		/**
		 * Processing time timers that are currently in-flight.
		 */
		private final PriorityQueue<InternalTimer<K, N>> processingTimeTimersQueue;
		private final Set<InternalTimer<K, N>> processingTimeTimers;

		/**
		 * Currently waiting watermark callbacks.
		 */
		private final Set<InternalTimer<K, N>> watermarkTimers;
		private final PriorityQueue<InternalTimer<K, N>> watermarkTimersQueue;

		public RestoredTimers(InputStream inputStream, ClassLoader userCodeClassLoader) throws Exception {

			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100);

			processingTimeTimers = new HashSet<>();
			processingTimeTimersQueue = new PriorityQueue<>(100);

			keySerializer = InstantiationUtil.deserializeObject(inputStream, userCodeClassLoader);
			namespaceSerializer = InstantiationUtil.deserializeObject(
					inputStream,
					userCodeClassLoader);

			DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inputStream);

			int numWatermarkTimers = inView.readInt();
			for (int i = 0; i < numWatermarkTimers; i++) {
				K key = keySerializer.deserialize(inView);
				N namespace = namespaceSerializer.deserialize(inView);
				long timestamp = inView.readLong();
				InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);
				watermarkTimers.add(timer);
				watermarkTimersQueue.add(timer);
			}

			int numProcessingTimeTimers = inView.readInt();
			for (int i = 0; i < numProcessingTimeTimers; i++) {
				K key = keySerializer.deserialize(inView);
				N namespace = namespaceSerializer.deserialize(inView);
				long timestamp = inView.readLong();
				InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);
				processingTimeTimersQueue.add(timer);
				processingTimeTimers.add(timer);
			}
		}
	}
}
