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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Emitter that handles event time synchronization between producer threads.
 *
 * <p>Records are organized into per producer queues that will block when capacity is exhausted.
 *
 * <p>Records are emitted by selecting the oldest available element of all producer queues,
 * as long as the timestamp does not exceed the current shared watermark plus allowed lookahead interval.
 *
 * @param <T>
 */
@Internal
public abstract class RecordEmitter<T extends TimestampedValue> implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(RecordEmitter.class);

	/**
	 * The default capacity of a single queue.
	 *
	 * <p>Larger queue size can lead to higher throughput, but also to
	 * very high heap space consumption, depending on the size of elements.
	 *
	 * <p>Note that this is difficult to tune, because it does not take into account
	 * the size of individual objects.
	 */
	public static final int DEFAULT_QUEUE_CAPACITY = 100;

	private final int queueCapacity;
	private final ConcurrentHashMap<Integer, AsyncRecordQueue<T>> queues = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<AsyncRecordQueue<T>, Boolean> emptyQueues = new ConcurrentHashMap<>();
	private final PriorityQueue<AsyncRecordQueue<T>> heads = new PriorityQueue<>(this::compareHeadElement);
	private volatile boolean running = true;
	private volatile long maxEmitTimestamp = Long.MAX_VALUE;
	private long maxLookaheadMillis = 60 * 1000; // one minute
	private long idleSleepMillis = 100;
	private final Object condition = new Object();

	public RecordEmitter(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	private int compareHeadElement(AsyncRecordQueue left, AsyncRecordQueue right) {
		return Long.compare(left.headTimestamp, right.headTimestamp);
	}

	/**
	 * Accepts records from readers.
	 *
	 * @param <T>
	 */
	public interface RecordQueue<T> {
		void put(T record) throws InterruptedException;

		int getSize();

		T peek();
	}

	private class AsyncRecordQueue<T> implements RecordQueue<T> {
		private final ArrayBlockingQueue<T> queue;
		private final int queueId;
		long headTimestamp;

		private AsyncRecordQueue(int queueId) {
			super();
			this.queue = new ArrayBlockingQueue<>(queueCapacity);
			this.queueId = queueId;
			this.headTimestamp = Long.MAX_VALUE;
		}

		@Override
		public void put(T record) throws InterruptedException {
			queue.put(record);
			synchronized (condition) {
				condition.notify();
			}
		}

		@Override
		public int getSize() {
			return queue.size();
		}

		@Override
		public T peek() {
			return queue.peek();
		}

	}

	/**
	 * The queue for the given producer (i.e. Kinesis shard consumer thread).
	 *
	 * <p>The producer may hold on to the queue for subsequent records.
	 *
	 * @param producerIndex
	 * @return
	 */
	public RecordQueue<T> getQueue(int producerIndex) {
		return queues.computeIfAbsent(producerIndex, (key) -> {
			AsyncRecordQueue<T> q = new AsyncRecordQueue<>(producerIndex);
			emptyQueues.put(q, false);
			return q;
		});
	}

	/**
	 * How far ahead of the watermark records should be emitted.
	 *
	 * <p>Needs to account for the latency of obtaining the global watermark.
	 *
	 * @param maxLookaheadMillis
	 */
	public void setMaxLookaheadMillis(long maxLookaheadMillis) {
		this.maxLookaheadMillis = maxLookaheadMillis;
		LOG.info("[setMaxLookaheadMillis] Max lookahead millis set to {}", maxLookaheadMillis);
	}

	/**
	 * Set the current watermark.
	 *
	 * <p>This watermark will be used to control which records to emit from the queues of pending
	 * elements. When an element timestamp is ahead of the watermark by at least {@link
	 * #maxLookaheadMillis}, elements in that queue will wait until the watermark advances.
	 *
	 * @param watermark
	 */
	public void setCurrentWatermark(long watermark) {
		maxEmitTimestamp = watermark + maxLookaheadMillis;
		synchronized (condition) {
			condition.notify();
		}
		LOG.info(
			"[setCurrentWatermark] Current watermark set to {}, maxEmitTimestamp = {}",
			watermark,
			maxEmitTimestamp);
	}

	/**
	 * Run loop, does not return unless {@link #stop()} was called.
	 */
	@Override
	public void run() {
		LOG.info("Starting emitter with maxLookaheadMillis: {}", this.maxLookaheadMillis);

		// emit available records, ordered by timestamp
		AsyncRecordQueue<T> min = heads.poll();
		runLoop:
		while (running) {
			// find a queue to emit from
			while (min == null) {
				// check new or previously empty queues
				if (!emptyQueues.isEmpty()) {
					for (AsyncRecordQueue<T> queueHead : emptyQueues.keySet()) {
						if (!queueHead.queue.isEmpty()) {
							emptyQueues.remove(queueHead);
							queueHead.headTimestamp = queueHead.queue.peek().getTimestamp();
							heads.offer(queueHead);
						}
					}
				}
				min = heads.poll();
				if (min == null) {
					synchronized (condition) {
						// wait for work
						try {
							condition.wait(idleSleepMillis);
						} catch (InterruptedException e) {
							continue runLoop;
						}
					}
				}
			}

			// wait until ready to emit min or another queue receives elements
			while (min.headTimestamp > maxEmitTimestamp) {
				synchronized (condition) {
					// wait until ready to emit
					try {
						condition.wait(idleSleepMillis);
					} catch (InterruptedException e) {
						continue runLoop;
					}
					if (min.headTimestamp > maxEmitTimestamp && !emptyQueues.isEmpty()) {
						// see if another queue can make progress
						heads.offer(min);
						min = null;
						continue runLoop;
					}
				}
			}

			// emit up to queue capacity records
			// cap on empty queues since older records may arrive
			AsyncRecordQueue<T> nextQueue = heads.poll();
			T record;
			int emitCount = 0;
			while ((record = min.queue.poll()) != null) {
				emit(record, min);
				// track last record emitted, even when queue becomes empty
				min.headTimestamp = record.getTimestamp();
				// potentially switch to next queue
				if ((nextQueue != null && min.headTimestamp > nextQueue.headTimestamp)
					|| (min.headTimestamp > maxEmitTimestamp)
					|| (emitCount++ > queueCapacity && !emptyQueues.isEmpty())) {
					break;
				}
			}
			if (record == null) {
				this.emptyQueues.put(min, true);
			} else {
				heads.offer(min);
			}
			min = nextQueue;
		}
	}

	public void stop() {
		running = false;
	}

	/** Emit the record. This is specific to a connector, like the Kinesis data fetcher. */
	public abstract void emit(T record, RecordQueue<T> source);

	/** Summary of emit queues that can be used for logging. */
	public String printInfo() {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("queues: %d, empty: %d",
			queues.size(), emptyQueues.size()));
		for (Map.Entry<Integer, AsyncRecordQueue<T>> e : queues.entrySet()) {
			AsyncRecordQueue q = e.getValue();
			sb.append(String.format("\n%d timestamp: %s size: %d",
				e.getValue().queueId, q.headTimestamp, q.queue.size()));
		}
		return sb.toString();
	}

}
