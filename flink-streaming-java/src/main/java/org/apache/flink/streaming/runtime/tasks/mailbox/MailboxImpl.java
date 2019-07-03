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
	 * Node for the embedded linked list that represents the mailbox queue.
	 */
	static final class Node {

		/** The payload. */
		final Runnable letter;

		/** Link to the next node in the list. */
		@Nullable
		Node next;

		Node(Runnable letter) {
			this.letter = letter;
		}
	}

	/**
	 * The head of the linked list of mailbox actions.
	 */
	@Nullable
	private Node head;

	/**
	 * The tail of the linked list of mailbox actions.
	 */
	@Nullable
	private Node tail;

	/**
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock;

	/**
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty;

	/**
	 * Number of letters in the mailbox.
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
		this.head = null;
		this.tail = null;
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
		final Node newTailNode = new Node(letter);
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			putTailInternal(newTailNode);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	@Override
	public void putFirst(@Nonnull Runnable priorityLetter) throws MailboxStateException {
		final ReentrantLock lock = this.lock;
		final Node newHeadNode = new Node(priorityLetter);
		lock.lock();
		try {
			putHeadInternal(newHeadNode);
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private void putHeadInternal(final Node newHead) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();

		if (head == null) {
			tail = newHead;
		} else {
			newHead.next = head;
		}

		head = newHead;

		incrementCountAndCheckOverflow();
		notEmpty.signal();
	}

	private void putTailInternal(final Node newTail) throws MailboxStateException {
		assert lock.isHeldByCurrentThread();
		checkPutStateConditions();

		if (tail == null) {
			head = newTail;
		} else {
			tail.next = newTail;
		}
		tail = newTail;

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

		if (head == null) {
			return null;
		}

		final Node oldHead = head;
		final Node newHead = head.next;

		head = newHead;

		if (newHead == null) {
			tail = null;
		}

		--count;
		return oldHead.letter;
	}

	private void drainAllLetters(List<Runnable> drainInto) {
		assert lock.isHeldByCurrentThread();
		Node localHead = head;
		while (localHead != null) {
			drainInto.add(localHead.letter);
			localHead = localHead.next;
		}
		count = 0;
		head = null;
		tail = null;
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
