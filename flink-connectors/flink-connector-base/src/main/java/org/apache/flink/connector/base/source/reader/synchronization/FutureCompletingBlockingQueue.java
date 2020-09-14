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

package org.apache.flink.connector.base.source.reader.synchronization;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A custom implementation of blocking queue with the following features.
 * <ul>
 *     <li>
 *         It allows a consuming thread to be notified asynchronously on element availability when the
 *         queue is empty.
 *     </li>
 *     <li>
 *         Allows the putting threads to be gracefully waken up without interruption.
 *     </li>
 * </ul>
 *
 * @param <T> the type of the elements in the queue.
 */
public class FutureCompletingBlockingQueue<T> {
	private final int capacity;
	private final FutureNotifier futureNotifier;

	/** The element queue. */
	private final Queue<T> queue;
	/** The lock for synchronization. */
	private final Lock lock;
	/** The per-thread conditions that are waiting on putting elements. */
	private final Queue<Condition> notFull;
	/** The shared conditions for getting elements. */
	private final Condition notEmpty;
	/** The per-thread conditions and wakeUp flags. */
	private ConditionAndFlag[] putConditionAndFlags;

	/**
	 * The default capacity for the queue.
	 */
	private static final Integer DEFAULT_CAPACITY = 1;

	public FutureCompletingBlockingQueue(FutureNotifier futureNotifier) {
		this(futureNotifier, DEFAULT_CAPACITY);
	}

	public FutureCompletingBlockingQueue(FutureNotifier futureNotifier, int capacity) {
		this.capacity = capacity;
		this.futureNotifier = futureNotifier;
		this.queue = new ArrayDeque<>(capacity);
		this.lock = new ReentrantLock();
		this.putConditionAndFlags = new ConditionAndFlag[1];
		this.notFull = new ArrayDeque<>();
		this.notEmpty = lock.newCondition();
	}

	/**
	 * Put an element into the queue. The thread blocks if the queue is full.
	 *
	 * @param threadIndex the index of the thread.
	 * @param element the element to put.
	 * @return true if the element has been successfully put into the queue, false otherwise.
	 * @throws InterruptedException when the thread is interrupted.
	 */
	public boolean put(int threadIndex, T element) throws InterruptedException {
		if (element == null) {
			throw new NullPointerException();
		}
		lock.lockInterruptibly();
		try {
			while (queue.size() >= capacity) {
				if (getAndResetWakeUpFlag(threadIndex)) {
					return false;
				}
				waitOnPut(threadIndex);
			}
			enqueue(element);
			return true;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get and remove the first element from the queue. The call blocks if the queue is empty.
	 *
	 * @return the first element in the queue.
	 * @throws InterruptedException when the thread is interrupted.
	 */
	public T take() throws InterruptedException{
		lock.lock();
		try {
			while (queue.size() == 0) {
				notEmpty.await();
			}
			return dequeue();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get and remove the first element from the queue. Null is retuned if the queue is empty.
	 *
	 * @return the first element from the queue, or Null if the queue is empty.
	 */
	public T poll() {
		lock.lock();
		try {
			if (queue.size() == 0) {
				return null;
			}
			return dequeue();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get the first element from the queue without removing it.
	 *
	 * @return the first element in the queue, or Null if the queue is empty.
	 */
	public T peek() {
		lock.lock();
		try {
			return queue.peek();
		} finally {
			lock.unlock();
		}
	}

	public int size() {
		lock.lock();
		try {
			return queue.size();
		} finally {
			lock.unlock();
		}
	}

	public boolean isEmpty() {
		lock.lock();
		try {
			return queue.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	public int remainingCapacity() {
		lock.lock();
		try {
			return capacity - queue.size();
		} finally {
			lock.unlock();
		}
	}

	public void wakeUpPuttingThread(int threadIndex) {
		lock.lock();
		try {
			maybeCreateCondition(threadIndex);
			ConditionAndFlag caf = putConditionAndFlags[threadIndex];
			if (caf != null) {
				caf.setWakeUp(true);
				caf.condition().signal();
			}
		} finally {
			lock.unlock();
		}
	}

	// --------------- private helpers -------------------------

	private void enqueue(T element) {
		int sizeBefore = queue.size();
		queue.add(element);
		futureNotifier.notifyComplete();
		if (sizeBefore == 0) {
			notEmpty.signal();
		}
		if (sizeBefore < capacity - 1 && !notFull.isEmpty()) {
			signalNextPutter();
		}
	}

	private T dequeue() {
		int sizeBefore = queue.size();
		T element = queue.poll();
		if (sizeBefore == capacity && !notFull.isEmpty()) {
			signalNextPutter();
		}
		if (sizeBefore > 1) {
			notEmpty.signal();
		}
		return element;
	}

	private void waitOnPut(int fetcherIndex) throws InterruptedException {
		maybeCreateCondition(fetcherIndex);
		Condition cond = putConditionAndFlags[fetcherIndex].condition();
		notFull.add(cond);
		cond.await();
	}

	private void signalNextPutter() {
		if (!notFull.isEmpty()) {
			notFull.poll().signal();
		}
	}

	private void maybeCreateCondition(int threadIndex) {
		if (putConditionAndFlags.length < threadIndex + 1) {
			putConditionAndFlags = Arrays.copyOf(putConditionAndFlags, threadIndex + 1);
		}

		if (putConditionAndFlags[threadIndex] == null) {
			putConditionAndFlags[threadIndex] = new ConditionAndFlag(lock.newCondition());
		}
	}

	private boolean getAndResetWakeUpFlag(int threadIndex) {
		maybeCreateCondition(threadIndex);
		if (putConditionAndFlags[threadIndex].getWakeUp()) {
			putConditionAndFlags[threadIndex].setWakeUp(false);
			return true;
		}
		return false;
	}

	// --------------- private per thread state ------------

	private static class ConditionAndFlag {
		private final Condition cond;
		private boolean wakeUp;

		private ConditionAndFlag(Condition cond) {
			this.cond = cond;
			this.wakeUp = false;
		}

		private Condition condition() {
			return cond;
		}

		private boolean getWakeUp() {
			return wakeUp;
		}

		private void setWakeUp(boolean value) {
			wakeUp = value;
		}
	}
}
