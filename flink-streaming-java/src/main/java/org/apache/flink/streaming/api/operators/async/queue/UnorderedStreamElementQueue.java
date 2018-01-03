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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.async.OperatorActions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unordered implementation of the {@link StreamElementQueue}. The unordered stream element queue
 * emits asynchronous results as soon as they are completed. Additionally it maintains the
 * watermark-stream record order. This means that no stream record can be overtaken by a watermark
 * and no watermark can overtake a stream record. However, stream records falling in the same
 * segment between two watermarks can overtake each other (their emission order is not guaranteed).
 */
@Internal
public class UnorderedStreamElementQueue implements StreamElementQueue {

	private static final Logger LOG = LoggerFactory.getLogger(UnorderedStreamElementQueue.class);

	/** Capacity of this queue. */
	private final int capacity;

	/** Executor to run the onComplete callbacks. */
	private final Executor executor;

	/** OperatorActions to signal the owning operator a failure. */
	private final OperatorActions operatorActions;

	/** Queue of uncompleted stream element queue entries segmented by watermarks. */
	private final ArrayDeque<Set<StreamElementQueueEntry<?>>> uncompletedQueue;

	/** Queue of completed stream element queue entries. */
	private final ArrayDeque<StreamElementQueueEntry<?>> completedQueue;

	/** First (chronologically oldest) uncompleted set of stream element queue entries. */
	private Set<StreamElementQueueEntry<?>> firstSet;

	// Last (chronologically youngest) uncompleted set of stream element queue entries. New
	// stream element queue entries are inserted into this set.
	private Set<StreamElementQueueEntry<?>> lastSet;
	private volatile int numberEntries;

	/** Locks and conditions for the blocking queue. */
	private final ReentrantLock lock;
	private final Condition notFull;
	private final Condition hasCompletedEntries;

	public UnorderedStreamElementQueue(
			int capacity,
			Executor executor,
			OperatorActions operatorActions) {

		Preconditions.checkArgument(capacity > 0, "The capacity must be larger than 0.");
		this.capacity = capacity;

		this.executor = Preconditions.checkNotNull(executor, "executor");

		this.operatorActions = Preconditions.checkNotNull(operatorActions, "operatorActions");

		this.uncompletedQueue = new ArrayDeque<>(capacity);
		this.completedQueue = new ArrayDeque<>(capacity);

		this.firstSet = new HashSet<>(capacity);
		this.lastSet = firstSet;

		this.numberEntries = 0;

		this.lock = new ReentrantLock();
		this.notFull = lock.newCondition();
		this.hasCompletedEntries = lock.newCondition();
	}

	@Override
	public <T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (numberEntries >= capacity) {
				notFull.await();
			}

			addEntry(streamElementQueueEntry);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public <T> boolean tryPut(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			if (numberEntries < capacity) {
				addEntry(streamElementQueueEntry);

				LOG.debug("Put element into unordered stream element queue. New filling degree " +
					"({}/{}).", numberEntries, capacity);

				return true;
			} else {
				LOG.debug("Failed to put element into unordered stream element queue because it " +
					"was full ({}/{}).", numberEntries, capacity);

				return false;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AsyncResult peekBlockingly() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (completedQueue.isEmpty()) {
				hasCompletedEntries.await();
			}

			LOG.debug("Peeked head element from unordered stream element queue with filling degree " +
				"({}/{}).", numberEntries, capacity);

			return completedQueue.peek();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AsyncResult poll() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (completedQueue.isEmpty()) {
				hasCompletedEntries.await();
			}

			numberEntries--;
			notFull.signalAll();

			LOG.debug("Polled element from unordered stream element queue. New filling degree " +
				"({}/{}).", numberEntries, capacity);

			return completedQueue.poll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Collection<StreamElementQueueEntry<?>> values() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			StreamElementQueueEntry<?>[] array = new StreamElementQueueEntry[numberEntries];

			array = completedQueue.toArray(array);

			int counter = completedQueue.size();

			for (StreamElementQueueEntry<?> entry: firstSet) {
				array[counter] = entry;
				counter++;
			}

			for (Set<StreamElementQueueEntry<?>> asyncBufferEntries : uncompletedQueue) {

				for (StreamElementQueueEntry<?> streamElementQueueEntry : asyncBufferEntries) {
					array[counter] = streamElementQueueEntry;
					counter++;
				}
			}

			return Arrays.asList(array);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		return numberEntries == 0;
	}

	@Override
	public int size() {
		return numberEntries;
	}

	/**
	 * Callback for onComplete events for the given stream element queue entry. Whenever a queue
	 * entry is completed, it is checked whether this entry belongs to the first set. If this is the
	 * case, then the element is added to the completed entries queue from where it can be consumed.
	 * If the first set becomes empty, then the next set is polled from the uncompleted entries
	 * queue. Completed entries from this new set are then added to the completed entries queue.
	 *
	 * @param streamElementQueueEntry which has been completed
	 * @throws InterruptedException if the current thread has been interrupted while performing the
	 * 	on complete callback.
	 */
	public void onCompleteHandler(StreamElementQueueEntry<?> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			if (firstSet.remove(streamElementQueueEntry)) {
				completedQueue.offer(streamElementQueueEntry);

				while (firstSet.isEmpty() && firstSet != lastSet) {
					firstSet = uncompletedQueue.poll();

					Iterator<StreamElementQueueEntry<?>> it = firstSet.iterator();

					while (it.hasNext()) {
						StreamElementQueueEntry<?> bufferEntry = it.next();

						if (bufferEntry.isDone()) {
							completedQueue.offer(bufferEntry);
							it.remove();
						}
					}
				}

				LOG.debug("Signal unordered stream element queue has completed entries.");
				hasCompletedEntries.signalAll();
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Add the given stream element queue entry to the current last set if it is not a watermark.
	 * If it is a watermark, then stop adding to the current last set, insert the watermark into its
	 * own set and add a new last set.
	 *
	 * @param streamElementQueueEntry to be inserted
	 * @param <T> Type of the stream element queue entry's result
	 */
	private <T> void addEntry(StreamElementQueueEntry<T> streamElementQueueEntry) {
		assert(lock.isHeldByCurrentThread());

		if (streamElementQueueEntry.isWatermark()) {
			lastSet = new HashSet<>(capacity);

			if (firstSet.isEmpty()) {
				firstSet.add(streamElementQueueEntry);
			} else {
				Set<StreamElementQueueEntry<?>> watermarkSet = new HashSet<>(1);
				watermarkSet.add(streamElementQueueEntry);
				uncompletedQueue.offer(watermarkSet);
			}
			uncompletedQueue.offer(lastSet);
		} else {
			lastSet.add(streamElementQueueEntry);
		}

		streamElementQueueEntry.onComplete(
			(StreamElementQueueEntry<T> value) -> {
				try {
					onCompleteHandler(value);
				} catch (InterruptedException e) {
					// The accept executor thread got interrupted. This is probably cause by
					// the shutdown of the executor.
					LOG.debug("AsyncBufferEntry could not be properly completed because the " +
						"executor thread has been interrupted.", e);
				} catch (Throwable t) {
					operatorActions.failOperator(new Exception("Could not complete the " +
						"stream element queue entry: " + value + '.', t));
				}
			},
			executor);

		numberEntries++;
	}
}
