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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MIN_PRIORITY;

/**
 * This class encapsulates the logic of the mailbox-based execution model. At the core of this model
 * {@link #runMailboxLoop()} that continuously executes the provided {@link MailboxDefaultAction} in a loop. On each
 * iteration, the method also checks if there are pending actions in the mailbox and executes such actions. This model
 * ensures single-threaded execution between the default action (e.g. record processing) and mailbox actions (e.g.
 * checkpoint trigger, timer firing, ...).
 *
 * <p>The {@link MailboxDefaultAction} interacts with this class through the {@link MailboxDefaultActionContext} to
 * communicate control flow changes to the mailbox loop, e.g. that invocations of the default action are temporarily
 * or permanently exhausted.
 *
 * <p>The design of {@link #runMailboxLoop()} is centered around the idea of keeping the expected hot path
 * (default action, no mail) as fast as possible, with just a single volatile read per iteration in
 * {@link TaskMailbox#hasMail}. This means that all checking of mail and other control flags (mailboxLoopRunning,
 * suspendedDefaultAction) are always connected to #hasMail indicating true. This means that control flag changes in
 * the mailbox thread can be done directly, but we must ensure that there is at least one action in the mailbox so that
 * the change is picked up. For control flag changes by all other threads, that must happen through mailbox actions,
 * this is automatically the case.
 *
 * <p>This class has a open-prepareClose-close lifecycle that is connected with and maps to the lifecycle of the
 * encapsulated {@link TaskMailbox} (which is open-quiesce-close).
 */
public class MailboxProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(MailboxProcessor.class);

	/** The mailbox data-structure that manages request for special actions, like timers, checkpoints, ... */
	private final TaskMailbox mailbox;

	/** Action that is repeatedly executed if no action request is in the mailbox. Typically record processing. */
	private final MailboxDefaultAction mailboxDefaultAction;

	/** The thread that executes the mailbox mails. */
	private final Thread mailboxThread;

	/** A pre-created instance of mailbox executor that executes all mails. */
	private final MailboxExecutor mainMailboxExecutor;

	/** Control flag to terminate the mailbox loop. Must only be accessed from mailbox thread. */
	private boolean mailboxLoopRunning;

	/**
	 * Remembers a currently active suspension of the default action. Serves as flag to indicate a suspended
	 * default action (suspended if not-null) and to reuse the object as return value in consecutive suspend attempts.
	 * Must only be accessed from mailbox thread.
	 */
	private SuspendedMailboxDefaultAction suspendedDefaultAction;

	/** Special action that is used to terminate the mailbox loop. */
	private final Runnable mailboxPoisonMail;

	public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction) {
		this.mailboxDefaultAction = Preconditions.checkNotNull(mailboxDefaultAction);
		this.mailbox = new TaskMailboxImpl();
		this.mailboxThread = Thread.currentThread();
		this.mainMailboxExecutor = new MailboxExecutorImpl(mailbox, mailboxThread, TaskMailbox.MIN_PRIORITY);
		this.mailboxPoisonMail = () -> mailboxLoopRunning = false;
		this.mailboxLoopRunning = true;
		this.suspendedDefaultAction = null;
	}

	/**
	 * Returns a pre-created executor service that executes all mails.
	 */
	public MailboxExecutor getMainMailboxExecutor() {
		return mainMailboxExecutor;
	}

	/**
	 * Returns an executor service facade to submit actions to the mailbox.
	 * @param priority
	 */
	public MailboxExecutor getMailboxExecutor(int priority) {
		return new MailboxExecutorImpl(mailbox, mailboxThread, priority);
	}

	/**
	 * Lifecycle method to close the mailbox for action submission.
	 */
	public void prepareClose() {
		mailbox.quiesce();
	}

	/**
	 * Lifecycle method to close the mailbox for action submission/retrieval. This will cancel all instances of
	 * {@link java.util.concurrent.RunnableFuture} that are still contained in the mailbox.
	 */
	public void close() {
		List<Runnable> droppedMails = mailbox.close();
		if (!droppedMails.isEmpty()) {
			LOG.debug("Closing the mailbox dropped mails {}.", droppedMails);
			FutureUtils.cancelRunnableFutures(droppedMails);
		}
	}

	private boolean isMailboxThread() {
		return Thread.currentThread() == mailboxThread;
	}

	/**
	 * Runs the mailbox processing loop. This is where the main work is done.
	 */
	public void runMailboxLoop() throws Exception {

		Preconditions.checkState(
			isMailboxThread(),
			"Method must be executed by declared mailbox thread!");

		final TaskMailbox localMailbox = mailbox;

		assert localMailbox.getState() == TaskMailbox.State.OPEN : "Mailbox must be opened!";

		final MailboxDefaultActionContext defaultActionContext = new MailboxDefaultActionContext(this);

		while (processMail(localMailbox)) {
			mailboxDefaultAction.runDefaultAction(defaultActionContext);
		}
	}

	/**
	 * Reports a throwable for rethrowing from the mailbox thread. This will clear and cancel all other pending mails.
	 * @param throwable to report by rethrowing from the mailbox loop.
	 */
	public void reportThrowable(Throwable throwable) {
		sendPriorityMail(
			() -> {
				throw new WrappingRuntimeException(throwable);
			},
			"Report throwable %s", throwable);
	}

	/**
	 * This method must be called to end the stream task when all actions for the tasks have been performed.
	 */
	public void allActionsCompleted() {
		mailbox.runExclusively(() -> {
			// keep state check and poison mail enqueuing atomic, such that no intermediate #close may cause a
			// MailboxStateException in #sendPriorityMail.
			if (mailbox.getState() == TaskMailbox.State.OPEN) {
				sendPriorityMail(mailboxPoisonMail, "poison mail");
			}
		});
	}

	private void sendPriorityMail(Runnable priorityMail, String descriptionFormat, Object... descriptionArgs) {
		mainMailboxExecutor.executeFirst(priorityMail, descriptionFormat, descriptionArgs);
	}

	/**
	 * This helper method handles all special actions from the mailbox. It returns true if the mailbox loop should
	 * continue running, false if it should stop. In the current design, this method also evaluates all control flag
	 * changes. This keeps the hot path in {@link #runMailboxLoop()} free from any other flag checking, at the cost
	 * that all flag changes must make sure that the mailbox signals mailbox#hasMail.
	 */
	private boolean processMail(TaskMailbox mailbox) throws InterruptedException {

		// Doing this check is an optimization to only have a volatile read in the expected hot path, locks are only
		// acquired after this point.
		if (!mailbox.hasMail()) {
			// We can also directly return true because all changes to #isMailboxLoopRunning must be connected to
			// mailbox.hasMail() == true.
			return true;
		}

		// TODO consider batched draining into list and/or limit number of executed mails
		// Take mails in a non-blockingly and execute them.
		Optional<Mail> maybeMail;
		while (isMailboxLoopRunning() && (maybeMail = mailbox.tryTake(MIN_PRIORITY)).isPresent()) {
			maybeMail.get().run();
		}

		// If the default action is currently not available, we can run a blocking mailbox execution until the default
		// action becomes available again.
		while (isDefaultActionUnavailable() && isMailboxLoopRunning()) {
			mailbox.take(MIN_PRIORITY).run();
		}

		return isMailboxLoopRunning();
	}

	/**
	 * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the default action,
	 * e.g. because there is currently no input available.
	 */
	private SuspendedMailboxDefaultAction suspendDefaultAction() {

		Preconditions.checkState(isMailboxThread(), "Suspending must only be called from the mailbox thread!");

		if (suspendedDefaultAction == null) {
			suspendedDefaultAction = new SuspendDefaultActionRunnable();
			ensureControlFlowSignalCheck();
		}

		return suspendedDefaultAction;
	}

	private boolean isDefaultActionUnavailable() {
		return suspendedDefaultAction != null;
	}

	private boolean isMailboxLoopRunning() {
		return mailboxLoopRunning;
	}

	/**
	 * Helper method to make sure that the mailbox loop will check the control flow flags in the next iteration.
	 */
	private void ensureControlFlowSignalCheck() {
		// Make sure that mailbox#hasMail is true via a dummy mail so that the flag change is noticed.
		if (!mailbox.hasMail()) {
			sendPriorityMail(() -> {}, "signal check");
		}
	}

	/**
	 * Implementation of {@link DefaultActionContext} that is connected to a {@link MailboxProcessor}
	 * instance.
	 */
	private static final class MailboxDefaultActionContext implements DefaultActionContext {

		private final MailboxProcessor mailboxProcessor;

		private MailboxDefaultActionContext(MailboxProcessor mailboxProcessor) {
			this.mailboxProcessor = mailboxProcessor;
		}

		@Override
		public void allActionsCompleted() {
			mailboxProcessor.allActionsCompleted();
		}

		@Override
		public SuspendedMailboxDefaultAction suspendDefaultAction() {
			return mailboxProcessor.suspendDefaultAction();
		}
	}

	/**
	 * Represents the suspended state of the default action and offers an idempotent method to resume execution.
	 */
	private final class SuspendDefaultActionRunnable implements SuspendedMailboxDefaultAction {

		@Override
		public void resume() {
			if (isMailboxThread()) {
				resumeInternal();
			} else {
				sendPriorityMail(this::resumeInternal, "resume default action");
			}
		}

		private void resumeInternal() {
			if (suspendedDefaultAction == this) {
				suspendedDefaultAction = null;
			}
		}
	}
}
