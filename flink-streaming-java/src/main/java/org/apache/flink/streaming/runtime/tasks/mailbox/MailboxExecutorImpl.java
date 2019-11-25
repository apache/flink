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
import org.apache.flink.streaming.runtime.tasks.ExecutionDecorator;
import org.apache.flink.util.Preconditions;

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

	public final ExecutionDecorator executionDecorator;

	public MailboxExecutorImpl(@Nonnull TaskMailbox mailbox, int priority, ExecutionDecorator executionDecorator) {
		this.mailbox = mailbox;
		this.priority = priority;
		this.executionDecorator = Preconditions.checkNotNull(executionDecorator);
	}

	@Override
	public void execute(
		@Nonnull final Runnable command,
		final String descriptionFormat,
		final Object... descriptionArgs) {
		try {
			mailbox.put(new Mail(command, priority, descriptionFormat, descriptionArgs));
		} catch (IllegalStateException mbex) {
			throw new RejectedExecutionException(mbex);
		}
	}

	@Override
	public void executeFirst(
		@Nonnull final Runnable command,
		final String descriptionFormat,
		final Object... descriptionArgs) {
		try {
			mailbox.putFirst(new Mail(command, priority, descriptionFormat, descriptionArgs));
		} catch (IllegalStateException mbex) {
			throw new RejectedExecutionException(mbex);
		}
	}

	@Override
	public void yield() throws InterruptedException {
		executionDecorator.dispatch(mailbox.take(priority));
	}

	@Override
	public boolean tryYield() {
		Optional<Mail> optionalMail = mailbox.tryTake(priority);
		if (optionalMail.isPresent()) {
			executionDecorator.dispatch(optionalMail.get());
			return true;
		} else {
			return false;
		}
	}
}
