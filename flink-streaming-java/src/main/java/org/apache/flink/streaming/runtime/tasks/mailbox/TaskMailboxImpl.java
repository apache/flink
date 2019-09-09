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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and tailored towards
 * our use case with multiple writers, single reader and volatile reads.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
	/**
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock;

	/**
	 * Internal queue of letters.
	 */
	@GuardedBy("lock")
	private final LinkedList<Mail> queue;

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

	public TaskMailboxImpl() {
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
	public Optional<Runnable> tryTakeMail(int priority) throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return Optional.ofNullable(takeHeadInternal(priority));
		} finally {
			lock.unlock();
		}
	}

	@Override
	public @Nonnull Runnable takeMail(int priorty) throws InterruptedException, MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			Runnable headLetter;
			while ((headLetter = takeHeadInternal(priorty)) == null) {
				notEmpty.await();
			}
			return headLetter;
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void putMail(@Nonnull Runnable letter, int priority) throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putTailInternal(new Mail(letter, priority));
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
			putHeadInternal(new Mail(priorityLetter, MAX_PRIORITY));
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private void putHeadInternal(Mail newHead) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();
		queue.addFirst(newHead);
		incrementCountAndCheckOverflow();
		notEmpty.signal();
	}

	private void putTailInternal(Mail newTail) throws MailboxStateException {
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
	private Runnable takeHeadInternal(int priority) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkTakeStateConditions();
		Iterator<Mail> iterator = queue.iterator();
		while (iterator.hasNext()) {
			Mail mail = iterator.next();
			if (mail.getPriority() >= priority) {
				--count;
				iterator.remove();
				return mail.getRunnable();
			}
		}
		return null;
	}

	private void drainAllLetters(List<Runnable> drainInto) {
		assert lock.isHeldByCurrentThread();
		for (Mail mail : queue) {
			drainInto.add(mail.getRunnable());
		}
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
			List<Runnable> droppedLetters = new ArrayList<>(count);
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

	@Override
	public Mailbox getDownstreamMailbox(int priority) {
		Preconditions.checkArgument(priority >= 0, "The priority of a downstream mailbox should be non-negative");
		return new DownstreamMailbox(priority);
	}

	class DownstreamMailbox implements Mailbox {
		private final int priority;

		DownstreamMailbox(int priority) {
			this.priority = priority;
		}

		@Override
		public Optional<Runnable> tryTakeMail() throws MailboxStateException {
			return TaskMailboxImpl.this.tryTakeMail(priority);
		}

		@Nonnull
		@Override
		public Runnable takeMail() throws InterruptedException, MailboxStateException {
			return TaskMailboxImpl.this.takeMail(priority);
		}

		@Override
		public void putMail(@Nonnull Runnable letter) throws MailboxStateException {
			TaskMailboxImpl.this.putMail(letter, priority);
		}

		@Override
		public void putFirst(@Nonnull Runnable priorityLetter) throws MailboxStateException {
			TaskMailboxImpl.this.putFirst(priorityLetter);
		}
	}

	/**
	 * An executable bound to a specific operator in the chain, such that it can be picked for downstream mailbox.
	 */
	static class Mail {
		private final Runnable runnable;
		private final int priority;

		public Mail(Runnable runnable, int priority) {
			this.runnable = runnable;
			this.priority = priority;
		}

		public int getPriority() {
			return priority;
		}

		Runnable getRunnable() {
			return runnable;
		}
	}
}
