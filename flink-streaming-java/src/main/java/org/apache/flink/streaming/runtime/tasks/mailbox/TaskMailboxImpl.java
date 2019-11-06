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
	 * Internal queue of mails.
	 */
	@GuardedBy("lock")
	private final LinkedList<Mail> queue;

	/**
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty;

	/**
	 * Number of mails in the mailbox. We track it separately from the queue#size to avoid locking on {@link #hasMail()}.
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
		this.state = State.OPEN;
		this.queue = new LinkedList<>();
		this.count = 0;
	}

	@Override
	public boolean hasMail() {
		return !isEmpty();
	}

	@Override
	public Optional<Mail> tryTake(int priority) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return Optional.ofNullable(takeHeadInternal(priority));
		} finally {
			lock.unlock();
		}
	}

	@Override
	public @Nonnull Mail take(int priorty) throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			Mail headMail;
			while ((headMail = takeHeadInternal(priorty)) == null) {
				notEmpty.await();
			}
			return headMail;
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void put(@Nonnull Mail mail) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putTailInternal(mail);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void putFirst(@Nonnull Mail mail) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putHeadInternal(mail);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private void putHeadInternal(Mail newHead) {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();
		queue.addFirst(newHead);
		incrementCountAndCheckOverflow();
		notEmpty.signal();
	}

	private void putTailInternal(Mail newTail) {
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
	private Mail takeHeadInternal(int priority) {
		assert lock.isHeldByCurrentThread();
		checkTakeStateConditions();
		Iterator<Mail> iterator = queue.iterator();
		while (iterator.hasNext()) {
			Mail mail = iterator.next();
			if (mail.getPriority() >= priority) {
				--count;
				iterator.remove();
				return mail;
			}
		}
		return null;
	}

	@Override
	public List<Mail> drain() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			List<Mail> drainedMails = new ArrayList<>(queue);
			queue.clear();
			return drainedMails;
		} finally {
			lock.unlock();
		}
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

	private void checkPutStateConditions() {
		final State state = this.state;
		if (!isPutAbleState()) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				State.OPEN + " for put operations.");
		}
	}

	private void checkTakeStateConditions() {
		final State state = this.state;
		if (!isTakeAbleState()) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				State.OPEN + " or " + State.QUIESCED + " for take operations.");
		}
	}

	@Override
	public void quiesce() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == State.OPEN) {
				state = State.QUIESCED;
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Nonnull
	@Override
	public List<Mail> close() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == State.CLOSED) {
				return Collections.emptyList();
			}
			List<Mail> droppedMails = drain();
			state = State.CLOSED;
			// to unblock all
			notEmpty.signalAll();
			return droppedMails;
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
	public void runExclusively(Runnable runnable) {
		lock.lock();
		try {
			runnable.run();
		} finally {
			lock.unlock();
		}
	}
}
