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
 * Consumer-facing side of the {@link Mailbox} interface. This is used to dequeue letters. The mailbox returns letters
 * in the order by which they were enqueued. A mailbox should only be consumed by one thread at a time.
 */
public interface MailboxReceiver {

	/**
	 * Returns <code>true</code> if the mailbox contains mail.
	 */
	boolean hasMail();

	/**
	 * Returns an optional with either the oldest letter from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 *
	 * @return an optional with either the oldest letter from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 * @throws  MailboxStateException if mailbox is already closed.
	 */
	Optional<Runnable> tryTakeMail() throws MailboxStateException;

	/**
	 * This method returns the oldest letter from the mailbox (head of queue) or blocks until a letter is available.
	 *
	 * @return the oldest letter from the mailbox (head of queue).
	 * @throws InterruptedException on interruption.
	 * @throws  MailboxStateException if mailbox is already closed.
	 */
	@Nonnull
	Runnable takeMail() throws InterruptedException, MailboxStateException;
}
