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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.MailboxExecutor;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

/**
 * A task mailbox provides read and write access to a mailbox and has a lifecycle of open -> (quiesced) -> closed. Mails
 * have a priority that can be used to retrieve only relevant letters.
 *
 * <h3>Threading model</h3>
 * The mailbox is bound to a mailbox thread passed during creation. Most operations may only occur through that thread.
 * Write operations ({@link #put(Mail)}, {@link #putFirst(Mail)}) can be executed by any thread. All other methods can
 * only be invoked by the mailbox thread, which is passed upon construction. To verify that the current thread is
 * allowed to take any mail, use {@link #isMailboxThread()}, but all methods will perform the check themselves and fail
 * accordingly if called from another thread.
 *
 * <h3>Life cycle</h3>
 * In the open state, the mailbox supports put and take operations. In the quiesced state, the mailbox supports only
 * take operations.
 *
 * <h3>Batch</h3>
 * A batch is a local view on the mailbox that does not contain simultaneously added mails similar to iterators of
 * copy-on-write collections.
 *
 * <p>A batch serves two purposes: it reduces synchronization if more than one mail is processable at the time of the
 * creation of a batch. Furthermore, it allows to divide the work of a mailbox in smaller logical chunks, such that the
 * task threads cannot be blocked by a mail that enqueues itself and thus provides input starvation.
 *
 * <p>A batch is created with {@link #createBatch()} and consumed with {@link #tryTakeFromBatch()}. Note that there is
 * no blocking {@code takeFromBatch} as batches can only be created and consumed from the * mailbox thread.
 *
 * <p>Also note that a batch can only be created in the {@link MailboxProcessor#runMailboxLoop()}. A batch must not
 * be extended in any of the consuming methods as we may run into task input starvation again. Assume a case where
 * the mailbox loop handles a timeout event that produces a record. That record is subsequently handled downstream,
 * where it may lead to a {@link MailboxExecutor#yield()} triggering another consumption method. If we extend the
 * batch in any way during that processing, we effectively lose the bounded processing guarantee of mails inside the
 * mailbox loop.
 */
@Internal
public interface TaskMailbox {
	/**
	 * The minimal priority for mails. The priority is used when no operator is associated with the mail.
	 */
	int MIN_PRIORITY = -1;
	/**
	 * The maximal priority for mails. This priority indicates that the message should be performed before any mail
	 * associated with an operator.
	 */
	int MAX_PRIORITY = Integer.MAX_VALUE;

	/**
	 * Check if the current thread is the mailbox thread.
	 *
	 * <p>Read operations will fail if they are called from another thread.
	 *
	 * @return only true if called from the mailbox thread.
	 */
	boolean isMailboxThread();

	/**
	 * Returns <code>true</code> if the mailbox contains mail.
	 *
	 * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
	 */
	boolean hasMail();

	/**
	 * Returns an optional with either the oldest mail from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 *
	 * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
	 *
	 * @return an optional with either the oldest mail from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 * @throws IllegalStateException if mailbox is already closed.
	 */
	Optional<Mail> tryTake(int priority);

	/**
	 * This method returns the oldest mail from the mailbox (head of queue) or blocks until a mail is available.
	 *
	 * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
	 *
	 * @return the oldest mail from the mailbox (head of queue).
	 * @throws InterruptedException on interruption.
	 * @throws IllegalStateException if mailbox is already closed.
	 */
	@Nonnull
	Mail take(int priority) throws InterruptedException;

	// --- Batch

	/**
	 * Creates a batch of mails that can be taken with {@link #tryTakeFromBatch()}. The batch does not affect
	 * {@link #tryTake(int)} and {@link #take(int)}; that is, they return the same mails even if no batch would have
	 * been created.
	 *
	 * <p>The default batch is empty. Thus, this method must be invoked once before {@link #tryTakeFromBatch()}.
	 *
	 * <p>If a batch is not completely consumed by {@link #tryTakeFromBatch()}, its elements are carried over to the
	 * new batch.
	 *
	 * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
	 *
	 * @return true if there is at least one element in the batch; that is, if there is any mail at all at the time
	 * of the invocation.
	 */
	boolean createBatch();

	/**
	 * Returns an optional with either the oldest mail from the batch (head of queue) if the batch is not empty or an
	 * empty optional otherwise.
	 *
	 * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
	 *
	 * <p>Note that there is no blocking {@code takeFromBatch} as batches can only be created and consumed from the
	 * mailbox thread.
	 *
	 * @return an optional with either the oldest mail from the batch (head of queue) if the batch is not empty or an
	 * 	empty optional otherwise.
	 * @throws IllegalStateException if mailbox is already closed.
	 */
	Optional<Mail> tryTakeFromBatch();

	// --- Write methods

	/**
	 * Enqueues the given mail to the mailbox and blocks until there is capacity for a successful put.
	 *
	 * <p>Mails can be added from any thread.
	 *
	 * @param mail the mail to enqueue.
	 * @throws IllegalStateException if the mailbox is quiesced or closed.
	 */
	void put(Mail mail);

	/**
	 * Adds the given action to the head of the mailbox.
	 *
	 * <p>Mails can be added from any thread.
	 *
	 * @param mail the mail to enqueue.
	 * @throws IllegalStateException if the mailbox is quiesced or closed.
	 */
	void putFirst(Mail mail);

	// --- Lifecycle methods

	/**
	 * This enum represents the states of the mailbox lifecycle.
	 */
	enum State {
		OPEN(true), QUIESCED(false), CLOSED(false);
		private final boolean acceptingMails;

		State(boolean acceptingMails) {
			this.acceptingMails = acceptingMails;
		}

		public boolean isAcceptingMails() {
			return acceptingMails;
		}
	}

	/**
	 * Drains the mailbox and returns all mails that were still enqueued.
	 *
	 * @return list with all mails that where enqueued in the mailbox.
	 */
	List<Mail> drain();

	/**
	 * Quiesce the mailbox. In this state, the mailbox supports only take operations and all pending and future put
	 * operations will throw {@link IllegalStateException}.
	 */
	void quiesce();

	/**
	 * Close the mailbox. In this state, all pending and future put operations and all pending and future take
	 * operations will throw {@link IllegalStateException}. Returns all mails that were still enqueued.
	 *
	 * @return list with all mails that where enqueued in the mailbox at the time of closing.
	 */
	@Nonnull
	List<Mail> close();

	/**
	 * Returns the current state of the mailbox as defined by the lifecycle enum {@link State}.
	 *
	 * @return the current state of the mailbox.
	 */
	@Nonnull
	State getState();

	/**
	 * Runs the given code exclusively on this mailbox. No synchronized operations can be run concurrently to the
	 * given runnable (e.g., {@link #put(Mail)} or modifying lifecycle methods).
	 *
	 * <p>Use this methods when you want to atomically execute code that uses different methods (e.g., check for
	 * state and then put message if open).
	 *
	 * @param runnable the runnable to execute
	 */
	void runExclusively(Runnable runnable);
}
