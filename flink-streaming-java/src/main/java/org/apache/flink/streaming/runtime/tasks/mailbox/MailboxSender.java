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

/**
 * Producer-facing side of the {@link Mailbox} interface. This is used to enqueue letters. Multiple producers threads
 * can put to the same mailbox.
 */
public interface MailboxSender {

	/**
	 * Enqueues the given letter to the mailbox and blocks until there is capacity for a successful put.
	 *
	 * <p>An optional description can be added to ease debugging and error-reporting. Any object can be passed on which
	 * {@link Object#toString()} is lazily invoked. In most cases, it should be a {@link String} or
	 * {@link org.apache.flink.streaming.runtime.tasks.mailbox.LazyString}. If no explicit description is taken, the
	 * command itself is used and {@code toString()} will be invoked on it.
	 *
	 * @param letter the letter to enqueue.
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putMail(@Nonnull Runnable letter, Object description) throws  MailboxStateException;

	/**
	 * Adds the given action to the head of the mailbox.
	 *
	 * <p>An optional description can be added to ease debugging and error-reporting. Any object can be passed on which
	 * {@link Object#toString()} is lazily invoked. In most cases, it should be a {@link String} or
	 * {@link org.apache.flink.streaming.runtime.tasks.mailbox.LazyString}. If no explicit description is taken, the
	 * command itself is used and {@code toString()} will be invoked on it.
	 *
	 * @param priorityLetter action to enqueue to the head of the mailbox.
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putFirst(@Nonnull Runnable priorityLetter, Object description) throws MailboxStateException;

}
