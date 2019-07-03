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
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock;

	/**
	 * Internal queue of letters.
	 */
	@GuardedBy("lock")
	private final LinkedList<Runnable> queue;

	/**
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty;

	/**
	 * Number of letters in the mailbox. We track it separately from the queue#size to avoid locking on {@link #hasMail()}.
	 */
	@GuardedBy("lock")
	private volatile int count;

	/**
	 * The state of the mailbox in the lifecycle of open, quiesced, and closed.
	 */
	@GuardedBy("lock")
	private volatile State state;

	public MailboxImpl() {
		this.lock = new ReentrantLock();
		this.notEmpty = lock.newCondition();
		this.state = State.CLOSED;
		this.queue = new LinkedList<>();
		this.count = 0;
	}

	@Override
	public boolean hasMail() {
		return !isEmpty();
	}

	@Override
	public Optional<Runnable> tryTakeMail() throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return Optional.ofNullable(takeHeadInternal());
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	@Override
	public Runnable takeMail() throws InterruptedException, MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			Runnable headLetter;
			while ((headLetter = takeHeadInternal()) == null) {
				notEmpty.await();
			}
			return headLetter;
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void putMail(@Nonnull Runnable letter) throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putTailInternal(letter);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void putFirst(@Nonnull Runnable priorityLetter) throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putHeadInternal(priorityLetter);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private void putHeadInternal(Runnable newHead) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();
		queue.addFirst(newHead);
		incrementCountAndCheckOverflow();
		notEmpty.signal();
	}

	private void putTailInternal(Runnable newTail) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();
		queue.addLast(newTail);
		incrementCountAndCheckOverflow();
		notEmpty.signal();
	}

	private void incrementCountAndCheckOverflow() {
		Preconditions.checkState(++count > 0, "Mailbox overflow.");
	}

	@Nullable
	private Runnable takeHeadInternal() throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkTakeStateConditions();
		Runnable oldHead = queue.pollFirst();
		if (oldHead != null) {
			--count;
		}
		return oldHead;
	}

	private void drainAllLetters(List<Runnable> drainInto) {
		assert lock.isHeldByCurrentThread();
		drainInto.addAll(queue);
		queue.clear();
		count = 0;
	}

	private boolean isEmpty() {
		return count == 0;
	}

	private boolean isPutAbleState() {
		return state == State.OPEN;
	}

	private boolean isTakeAbleState() {
		return state != State.CLOSED;
	}

	private void checkPutStateConditions() throws MailboxStateException {
		final State state = this.state;
		if (!isPutAbleState()) {
			throw new MailboxStateException("Mailbox is in state " + state + ", but is required to be in state " +
				State.OPEN + " for put operations.");
		}
	}

	private void checkTakeStateConditions() throws MailboxStateException {
		final State state = this.state;
		if (!isTakeAbleState()) {
			throw new MailboxStateException("Mailbox is in state " + state + ", but is required to be in state " +
				State.OPEN + " or " + State.QUIESCED + " for take operations.");
		}
	}

	@Override
	public void open() {
		lock.lock();
		try {
			if (state == State.CLOSED) {
				state = State.OPEN;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void quiesce() {
		lock.lock();
		try {
			if (state == State.OPEN) {
				state = State.QUIESCED;
			}
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	@Override
	public List<Runnable> close() {
		lock.lock();
		try {
			if (state == State.CLOSED) {
				return Collections.emptyList();
			}
			ArrayList<Runnable> droppedLetters = new ArrayList<>(count);
			drainAllLetters(droppedLetters);
			state = State.CLOSED;
			// to unblock all
			notEmpty.signalAll();
			return droppedLetters;
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	@Override
	public State getState() {
		return state;
	}
}
