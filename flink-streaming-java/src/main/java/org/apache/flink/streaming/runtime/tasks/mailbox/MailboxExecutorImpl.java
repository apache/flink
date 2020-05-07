/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

/**
 * Implementation of an executor service build around a mailbox-based execution model.
 */
@Internal
public final class MailboxExecutorImpl implements MailboxExecutor {

	/** The mailbox that manages the submitted runnable objects. */
	@Nonnull
	private final TaskMailbox mailbox;

	private final int priority;

	private final StreamTaskActionExecutor actionExecutor;

	public MailboxExecutorImpl(@Nonnull TaskMailbox mailbox, int priority, StreamTaskActionExecutor actionExecutor) {
		this.mailbox = mailbox;
		this.priority = priority;
		this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
	}

	@Override
	public void execute(
			final ThrowingRunnable<? extends Exception> command,
			final String descriptionFormat,
			final Object... descriptionArgs) {
		try {
			mailbox.put(new Mail(command, priority, actionExecutor, descriptionFormat, descriptionArgs));
		} catch (IllegalStateException mbex) {
			throw new RejectedExecutionException(mbex);
		}
	}

	@Override
	public void yield() throws InterruptedException {
		Mail mail = mailbox.take(priority);
		try {
			mail.run();
		} catch (Exception ex) {
			throw WrappingRuntimeException.wrapIfNecessary(ex);
		}
	}

	@Override
	public boolean tryYield() {
		Optional<Mail> optionalMail = mailbox.tryTake(priority);
		if (optionalMail.isPresent()) {
			try {
				optionalMail.get().run();
			} catch (Exception ex) {
				throw WrappingRuntimeException.wrapIfNecessary(ex);
			}
			return true;
		} else {
			return false;
		}
	}
}
