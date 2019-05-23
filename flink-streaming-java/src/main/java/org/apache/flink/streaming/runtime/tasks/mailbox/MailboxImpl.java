/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of {@link Mailbox} inspired by {@link java.util.concurrent.ArrayBlockingQueue} and tailored towards
 * our use case with multiple writers, single reader and volatile reads instead of lock & read on {@link #count}.
 */
@ThreadSafe
public class MailboxImpl implements Mailbox {

	/**
	 * The enqueued letters.
	 */
	@GuardedBy("lock")
	private final Runnable[] ringBuffer;

	/**
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock;

	/**
	 * Condition that is triggered when the buffer is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty;

	/**
	 * Condition that is triggered when the buffer is no longer full.
	 */
	@GuardedBy("lock")
	private final Condition notFull;

	/**
	 * Index of the ring buffer head.
	 */
	@GuardedBy("lock")
	private int headIndex;

	/**
	 * Index of the ring buffer tail.
	 */
	@GuardedBy("lock")
	private int tailIndex;

	/**
	 * Number of letters in the mailbox.
	 */
	@GuardedBy("lock")
	private volatile int count;

	/**
	 * A mask to wrap around the indexes of the ring buffer. We use this to avoid ifs or modulo ops.
	 */
	private final int moduloMask;

	public MailboxImpl() {
		this(6); // 2^6 = 64
	}

	public MailboxImpl(int capacityPow2) {
		final int capacity = 1 << capacityPow2;
		Preconditions.checkState(capacity > 0);
		this.moduloMask = capacity - 1;
		this.ringBuffer = new Runnable[capacity];
		this.lock = new ReentrantLock();
		this.notEmpty = lock.newCondition();
		this.notFull = lock.newCondition();
	}

	@Override
	public boolean hasMail() {
		return !isEmpty();
	}

	@Override
	public Optional<Runnable> tryTakeMail() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return isEmpty() ? Optional.empty() : Optional.of(takeInternal());
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	@Override
	public Runnable takeMail() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (isEmpty()) {
				notEmpty.await();
			}
			return takeInternal();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void waitUntilHasMail() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (isEmpty()) {
				notEmpty.await();
			}
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public boolean tryPutMail(@Nonnull Runnable letter) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isFull()) {
				return false;
			} else {
				putInternal(letter);
				return true;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void putMail(@Nonnull Runnable letter) throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (isFull()) {
				notFull.await();
			}
			putInternal(letter);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void waitUntilHasCapacity() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (isFull()) {
				notFull.await();
			}
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private void putInternal(Runnable letter) {
		assert lock.isHeldByCurrentThread();
		this.ringBuffer[tailIndex] = letter;
		tailIndex = increaseIndexWithWrapAround(tailIndex);
		++count;
		notEmpty.signal();
	}

	private Runnable takeInternal() {
		assert lock.isHeldByCurrentThread();
		final Runnable[] buffer = this.ringBuffer;
		Runnable letter = buffer[headIndex];
		buffer[headIndex] = null;
		headIndex = increaseIndexWithWrapAround(headIndex);
		--count;
		notFull.signal();
		return letter;
	}

	private int increaseIndexWithWrapAround(int old) {
		return (old + 1) & moduloMask;
	}

	private boolean isFull() {
		return count >= ringBuffer.length;
	}

	private boolean isEmpty() {
		return count == 0;
	}

	@Override
	public void clearAndPut(@Nonnull Runnable shutdownAction) {
		lock.lock();
		try {
			int localCount = count;
			while (localCount > 0) {
				ringBuffer[headIndex] = null;
				headIndex = increaseIndexWithWrapAround(headIndex);
				--localCount;
			}
			count = 0;
			putInternal(shutdownAction);
		} finally {
			lock.unlock();
		}
	}
}
