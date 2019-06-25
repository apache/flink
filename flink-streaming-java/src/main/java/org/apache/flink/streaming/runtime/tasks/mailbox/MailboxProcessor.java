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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * This class encapsulates the logic of the mailbox-based execution model.
 */
public class MailboxProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(MailboxProcessor.class);

	private final Runnable DEFAULT_ACTION_AVAILABLE = () -> {};

	private final Mailbox mailbox;
	private final TaskMailboxExecutorService taskMailboxExecutor;
	private final MailboxDefaultAction mailboxDefaultAction;

	private boolean mailboxLoopRunning;
	private final Runnable mailboxPoisonLetter;

	public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction) {
		this.mailboxDefaultAction = mailboxDefaultAction;
		this.mailbox = new MailboxImpl();
		this.taskMailboxExecutor = new TaskMailboxExecutorServiceImpl(mailbox);

		this.mailboxPoisonLetter = () -> mailboxLoopRunning = false;
		this.mailboxLoopRunning = true;
	}

	public TaskMailboxExecutorService getTaskMailboxExecutor() {
		return taskMailboxExecutor;
	}

	public void open() {
		mailbox.open();
	}

	public void prepareClose() {
		taskMailboxExecutor.shutdown();
	}

	public void close() {
		FutureUtils.cancelRunnableFutures(taskMailboxExecutor.shutdownNow());
	}

	public void runMailboxLoop() throws Exception {

		assert taskMailboxExecutor.isMailboxThread() :
			"StreamTask::run must be executed by declared mailbox thread!";

		final Mailbox localMailbox = mailbox;

		assert localMailbox.getState() == Mailbox.State.OPEN : "Mailbox must be opened!";

		final MailboxDefaultActionContext defaultActionContext = new MailboxDefaultActionContext();

		while (processMail(localMailbox)) {
			mailboxDefaultAction.runDefaultAction(defaultActionContext);
		}
	}

	public void switchToLegacySourceCompatibilityMailboxLoop(
		final Object checkpointLock) throws MailboxStateException, InterruptedException {

		assert taskMailboxExecutor.isMailboxThread() :
			"Legacy source compatibility mailbox loop must run in mailbox thread!";

		while (isMailboxLoopRunning()) {

			Runnable letter = mailbox.takeMail();

			synchronized (checkpointLock) {
				letter.run();
			}
		}
	}

	public void cancelMailboxExecution() {
		try {
			List<Runnable> droppedRunnables = mailbox.clearAndPut(mailboxPoisonLetter);
			FutureUtils.cancelRunnableFutures(droppedRunnables);
		} catch (MailboxStateException msex) {
			LOG.debug("Mailbox already closed in cancel().", msex);
		}
	}

	/**
	 * This method must be called to end the stream task when all actions for the tasks have been performed.
	 */
	public void allActionsCompleted() {
		try {
			if (taskMailboxExecutor.isMailboxThread()) {
				if (!mailbox.tryPutFirst(mailboxPoisonLetter)) {
					// mailbox is full - in this particular case we know for sure that we will still run through the
					// break condition check inside the mailbox loop and so we can just run directly.
					mailboxPoisonLetter.run();
				}
			} else {
				mailbox.putFirst(mailboxPoisonLetter);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (MailboxStateException me) {
			LOG.debug("Action context could not submit poison letter to mailbox.", me);
		}
	}

	private boolean processMail(Mailbox mailbox) throws MailboxStateException {

		if (!mailbox.hasMail()) {
			return true;
		}

		// TODO consider batched draining into list and/or limit number of executed letters
		Optional<Runnable> maybeLetter;
		while (isMailboxLoopRunning() && (maybeLetter = mailbox.tryTakeMail()).isPresent()) {
			maybeLetter.get().run();
		}

		return isMailboxLoopRunning();
	}

	private boolean isMailboxLoopRunning() {
		return mailboxLoopRunning;
	}

	private final class MailboxDefaultActionContext implements MailboxDefaultAction.ActionContext {

		/** Special value, letter that "wakes up" a waiting mailbox loop. */
		private final Runnable actionUnavailableLetter;

		MailboxDefaultActionContext() {
			this.actionUnavailableLetter = ThrowingRunnable.unchecked(() -> mailbox.takeMail().run());
		}

		@Override
		public void allActionsCompleted() {
			MailboxProcessor.this.allActionsCompleted();
		}

		/**
		 * Calling this method signals that the mailbox-thread should continue invoking the default action, e.g. because
		 * new input became available for processing.
		 */
		public void actionsAvailable() {
			putOrExecuteDirectly(DEFAULT_ACTION_AVAILABLE);
		}

		/**
		 * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the default action,
		 * e.g. because there is currently no input available.
		 */
		public void actionsUnavailable() {
			putOrExecuteDirectly(actionUnavailableLetter);
		}

		private void putOrExecuteDirectly(Runnable letter) {
			try {
				if (taskMailboxExecutor.isMailboxThread()) {
					if (!mailbox.tryPutMail(letter)) {
						letter.run();
					}
				} else {
					mailbox.putMail(letter);
				}
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			} catch (MailboxStateException me) {
				LOG.debug("Action context could not submit letter {} to mailbox.", letter, me);
			}
		}
	}
}
