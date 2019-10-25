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

import java.util.Optional;

/**
 * A mailbox is basically a queue for inter-thread message exchange in form of {@link Runnable} objects between multiple
 * producer threads and a single consumer.
 *
 * <p>This interface combines the {@link Mailbox} and {@link Mailbox} side without life-cycle methods.
 *
 * @see TaskMailbox
 */
public interface Mailbox {
	/**
	 * Returns an optional with either the oldest mail from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 *
	 * @return an optional with either the oldest mail from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 * @throws IllegalStateException if mailbox is already closed.
	 */
	Optional<Mail> tryTake(int priority);

	/**
	 * This method returns the oldest mail from the mailbox (head of queue) or blocks until a mail is available.
	 *
	 * @return the oldest mail from the mailbox (head of queue).
	 * @throws InterruptedException on interruption.
	 * @throws IllegalStateException if mailbox is already closed.
	 */
	@Nonnull
	Mail take(int priority) throws InterruptedException;

	/**
	 * Enqueues the given mail to the mailbox and blocks until there is capacity for a successful put.
	 *
	 * @param mail the mail to enqueue.
	 * @throws IllegalStateException if the mailbox is quiesced or closed.
	 */
	void put(Mail mail);

	/**
	 * Adds the given action to the head of the mailbox.
	 *
	 * @param mail the mail to enqueue.
	 * @throws IllegalStateException if the mailbox is quiesced or closed.
	 */
	void putFirst(Mail mail);
}
