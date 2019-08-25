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

package org.apache.flink.streaming.runtime.tasks.mailbox.execution;

import org.apache.flink.streaming.runtime.tasks.mailbox.Mailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxStateException;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of an executor service build around a mailbox-based execution model.
 */
public class MailboxExecutorServiceImpl extends AbstractExecutorService implements MailboxExecutorService {

	/** Reference to the thread that executes the mailbox letters.  */
	@Nonnull
	private final Thread taskMailboxThread;

	/** The mailbox that manages the submitted runnable objects. */
	@Nonnull
	private final Mailbox mailbox;

	public MailboxExecutorServiceImpl(@Nonnull Mailbox mailbox) {
		this(mailbox, Thread.currentThread());
	}

	public MailboxExecutorServiceImpl(@Nonnull Mailbox mailbox, @Nonnull Thread taskMailboxThread) {
		this.mailbox = mailbox;
		this.taskMailboxThread = taskMailboxThread;
	}

	@Override
	public void execute(@Nonnull Runnable command) {
		try {
			mailbox.putMail(command);
		} catch (MailboxStateException mbex) {
			throw new RejectedExecutionException(mbex);
		}
	}

	@Override
	public void yield() throws InterruptedException, IllegalStateException {
		checkIsMailboxThread();
		try {
			Runnable runnable = mailbox.takeMail();
			runnable.run();
		} catch (MailboxStateException e) {
			throw new IllegalStateException("Mailbox can no longer supply runnables for yielding.", e);
		}
	}

	@Override
	public boolean tryYield() throws IllegalStateException {
		checkIsMailboxThread();
		try {
			Optional<Runnable> runnableOptional = mailbox.tryTakeMail();
			if (runnableOptional.isPresent()) {
				runnableOptional.get().run();
				return true;
			} else {
				return false;
			}
		} catch (MailboxStateException e) {
			throw new IllegalStateException("Mailbox can no longer supply runnables for yielding.", e);
		}
	}

	@Override
	public boolean isMailboxThread() {
		return Thread.currentThread() == taskMailboxThread;
	}

	@Override
	public void shutdown() {
		mailbox.quiesce();
	}

	@Nonnull
	@Override
	public List<Runnable> shutdownNow() {
		return mailbox.close();
	}

	@Override
	public boolean isShutdown() {
		return mailbox.getState() != Mailbox.State.OPEN;
	}

	@Override
	public boolean isTerminated() {
		return mailbox.getState() == Mailbox.State.CLOSED;
	}

	@Override
	public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
		throw new UnsupportedOperationException("This method is not supported by this implementation.");
	}

	/**
	 * Returns the mailbox that manages the execution order.
	 *
	 * @return the mailbox.
	 */
	@Nonnull
	public Mailbox getMailbox() {
		return mailbox;
	}

	private void checkIsMailboxThread() {
		if (!isMailboxThread()) {
			throw new IllegalStateException(
				"Illegal thread detected. This method must be called from inside the mailbox thread!");
		}
	}
}
