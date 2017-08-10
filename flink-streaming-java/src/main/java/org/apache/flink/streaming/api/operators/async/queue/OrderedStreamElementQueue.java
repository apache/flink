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
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ordered {@link StreamElementQueue} implementation. The ordered stream element queue emits
 * asynchronous results in the order in which the {@link StreamElementQueueEntry} have been added
 * to the queue. Thus, even if the completion order can be arbitrary, the output order strictly
 * follows the insertion order (element cannot overtake each other).
 */
@Internal
public class OrderedStreamElementQueue implements StreamElementQueue {

	private static final Logger LOG = LoggerFactory.getLogger(OrderedStreamElementQueue.class);

	/** Capacity of this queue. */
	private final int capacity;

	/** Executor to run the onCompletion callback. */
	private final Executor executor;

	/** Operator actions to signal a failure to the operator. */
	private final OperatorActions operatorActions;

	/** Lock and conditions for the blocking queue. */
	private final ReentrantLock lock;
	private final Condition notFull;
	private final Condition headIsCompleted;

	/** Queue for the inserted StreamElementQueueEntries. */
	private final ArrayDeque<StreamElementQueueEntry<?>> queue;

	public OrderedStreamElementQueue(
			int capacity,
			Executor executor,
			OperatorActions operatorActions) {

		Preconditions.checkArgument(capacity > 0, "The capacity must be larger than 0.");
		this.capacity = capacity;

		this.executor = Preconditions.checkNotNull(executor, "executor");

		this.operatorActions = Preconditions.checkNotNull(operatorActions, "operatorActions");

		this.lock = new ReentrantLock(false);
		this.headIsCompleted = lock.newCondition();
		this.notFull = lock.newCondition();

		this.queue = new ArrayDeque<>(capacity);
	}

	@Override
	public AsyncResult peekBlockingly() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (queue.isEmpty() || !queue.peek().isDone()) {
				headIsCompleted.await();
			}

			LOG.debug("Peeked head element from ordered stream element queue with filling degree " +
				"({}/{}).", queue.size(), capacity);

			return queue.peek();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public AsyncResult poll() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (queue.isEmpty() || !queue.peek().isDone()) {
				headIsCompleted.await();
			}

			notFull.signalAll();

			LOG.debug("Polled head element from ordered stream element queue. New filling degree " +
				"({}/{}).", queue.size() - 1, capacity);

			return queue.poll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Collection<StreamElementQueueEntry<?>> values() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			StreamElementQueueEntry<?>[] array = new StreamElementQueueEntry[queue.size()];

			array = queue.toArray(array);

			return Arrays.asList(array);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public <T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			while (queue.size() >= capacity) {
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
			if (queue.size() < capacity) {
				addEntry(streamElementQueueEntry);

				LOG.debug("Put element into ordered stream element queue. New filling degree " +
					"({}/{}).", queue.size(), capacity);

				return true;
			} else {
				LOG.debug("Failed to put element into ordered stream element queue because it " +
					"was full ({}/{}).", queue.size(), capacity);

				return false;
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Add the given {@link StreamElementQueueEntry} to the queue. Additionally, this method
	 * registers a onComplete callback which is triggered once the given queue entry is completed.
	 *
	 * @param streamElementQueueEntry to be inserted
	 * @param <T> Type of the stream element queue entry's result
	 */
	private <T> void addEntry(StreamElementQueueEntry<T> streamElementQueueEntry) {
		assert(lock.isHeldByCurrentThread());

		queue.addLast(streamElementQueueEntry);

		streamElementQueueEntry.onComplete(
			(StreamElementQueueEntry<T> value) -> {
				try {
					onCompleteHandler(value);
				} catch (InterruptedException e) {
					// we got interrupted. This indicates a shutdown of the executor
					LOG.debug("AsyncBufferEntry could not be properly completed because the " +
						"executor thread has been interrupted.", e);
				} catch (Throwable t) {
					operatorActions.failOperator(new Exception("Could not complete the " +
						"stream element queue entry: " + value + '.', t));
				}
			},
			executor);
	}

	/**
	 * Check if the completed {@link StreamElementQueueEntry} is the current head. If this is the
	 * case, then notify the consumer thread about a new consumable entry.
	 *
	 * @param streamElementQueueEntry which has been completed
	 * @throws InterruptedException if the current thread is interrupted
	 */
	private void onCompleteHandler(StreamElementQueueEntry<?> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			if (!queue.isEmpty() && queue.peek().isDone()) {
				LOG.debug("Signal ordered stream element queue has completed head element.");
				headIsCompleted.signalAll();
			}
		} finally {
			lock.unlock();
		}
	}
}
