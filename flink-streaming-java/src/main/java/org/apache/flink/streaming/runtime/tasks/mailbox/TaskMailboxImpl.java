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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.*;

/**
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and tailored towards
 * our use case with multiple writers and single reader.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
	/**
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock = new ReentrantLock();

	/**
	 * Internal queue of mails.
	 */
	@GuardedBy("lock")
	private final Deque<Mail> queue = new ArrayDeque<>();

	/**
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty = lock.newCondition();

	/**
	 * The state of the mailbox in the lifecycle of open, quiesced, and closed.
	 */
	@GuardedBy("lock")
	private State state = OPEN;

	@Override
	public boolean hasMail() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return !queue.isEmpty();
		} finally {
			lock.unlock();
		}
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
		notEmpty.signal();
	}

	private void putTailInternal(Mail newTail) {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();
		queue.addLast(newTail);
		notEmpty.signal();
	}

	@Nullable
	private Mail takeHeadInternal(int priority) {
		assert lock.isHeldByCurrentThread();
		checkTakeStateConditions();
		Iterator<Mail> iterator = queue.iterator();
		while (iterator.hasNext()) {
			Mail mail = iterator.next();
			if (mail.getPriority() >= priority) {
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

	private void checkPutStateConditions() {
		final State state = this.state;
		if (this.state != OPEN) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " for put operations.");
		}
	}

	private void checkTakeStateConditions() {
		final State state = this.state;
		if (this.state == CLOSED) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " or " + QUIESCED + " for take operations.");
		}
	}

	@Override
	public void quiesce() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == OPEN) {
				state = QUIESCED;
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
			if (state == CLOSED) {
				return Collections.emptyList();
			}
			List<Mail> droppedMails = drain();
			state = CLOSED;
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
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return state;
		} finally {
			lock.unlock();
		}
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
