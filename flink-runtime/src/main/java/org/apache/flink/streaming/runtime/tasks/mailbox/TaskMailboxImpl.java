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

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.CLOSED;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.OPEN;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.QUIESCED;

/**
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and
 * tailored towards our use case with multiple writers and single reader.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
    /** Lock for all concurrent ops. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Internal queue of mails. */
    @GuardedBy("lock")
    private final Deque<Mail> queue = new ArrayDeque<>();

    /** Condition that is triggered when the mailbox is no longer empty. */
    @GuardedBy("lock")
    private final Condition notEmpty = lock.newCondition();

    /** The state of the mailbox in the lifecycle of open, quiesced, and closed. */
    @GuardedBy("lock")
    private State state = OPEN;

    /** Reference to the thread that executes the mailbox mails. */
    @Nonnull private final Thread taskMailboxThread;

    /**
     * The current batch of mails. A new batch can be created with {@link #tryBuildBatch()} and
     * consumed with {@link #tryTakeFromBatch()}.
     */
    private final Deque<Mail> batch = new ArrayDeque<>();

    /**
     * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of
     * {@link #batch}.
     */
    private volatile boolean hasNewMail = false;

    /**
     * Performance optimization where there is new urgent mail. When there is no urgent mail in the
     * batch, it should be checked every time mail is taken, including taking mail from batch queue.
     */
    private volatile boolean hasNewUrgentMail = false;

    public TaskMailboxImpl(@Nonnull final Thread taskMailboxThread) {
        this.taskMailboxThread = taskMailboxThread;
    }

    @VisibleForTesting
    public TaskMailboxImpl() {
        this(Thread.currentThread());
    }

    @Override
    public boolean isMailboxThread() {
        return Thread.currentThread() == taskMailboxThread;
    }

    @Override
    public boolean hasMail() {
        checkIsMailboxThread();
        return !batch.isEmpty() || hasNewMail;
    }

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return batch.size() + queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTake(int priority) {
        checkIsMailboxThread();
        checkTakeStateConditions();

        moveUrgentMailsToBatchIfNeeded(true);

        Mail head = takeOrNull(batch, priority);
        if (head != null) {
            return Optional.of(head);
        }
        if (!hasNewMail) {
            return Optional.empty();
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Mail value = takeOrNull(queue, priority);
            if (value == null) {
                return Optional.empty();
            }
            updateNewMailFlags();
            return Optional.ofNullable(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public @Nonnull Mail take(int priority) throws InterruptedException, IllegalStateException {
        checkIsMailboxThread();
        checkTakeStateConditions();

        moveUrgentMailsToBatchIfNeeded(true);

        Mail head = takeOrNull(batch, priority);
        if (head != null) {
            return head;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            Mail headMail;
            while ((headMail = takeOrNull(queue, priority)) == null) {
                // to ease debugging
                notEmpty.await(1, TimeUnit.SECONDS);
            }
            updateNewMailFlags();
            return headMail;
        } finally {
            lock.unlock();
        }
    }

    private void updateNewMailFlags() {
        Mail peek = queue.peek();
        if (peek != null) {
            hasNewMail = true;
            hasNewUrgentMail = peek.getMailOptions().isUrgent();
        } else {
            hasNewMail = false;
            hasNewUrgentMail = false;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean createBatch() {
        moveUrgentMailsToBatchIfNeeded(false);
        return !batch.isEmpty();
    }

    /**
     * Try to create a batch of mails that can be taken with {@link #tryTakeFromBatch()}. The batch
     * does not affect {@link #tryTake(int)} and {@link #take(int)}; that is, they return the same
     * mails even if no batch had been created.
     *
     * <p>If a batch is not completely consumed by {@link #tryTakeFromBatch()}, its elements are
     * carried over to the new batch.
     *
     * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
     *
     * <p>To ensure that urgent mails are executed in FIFO order, moving mails from queue to batch
     * only happens when batch doesn't have urgent mails. In addition, in order to reduce the
     * frequent acquisition of locks caused by moving, moving is not required if batch is not empty
     * and no new urgent mails.
     *
     * <pre>{@code
     * All received mails (from head to tail):
     *   Urgent mails:     MailA -> MailB -> MailC -> MailD -> MailE -> MailF
     *   Non-urgent mails: Mail1 -> Mail2 -> Mail3 -> Mail4 -> Mail5 -> Mail6 -> Mail7
     *
     * All received mails (The actual storage structure):
     *   queue: UrgentMails and NonUrgentMails (UrgentMails is in reverse order)
     *      MailF - MailE - MailD - Mail6 - Mail7 (the second batch)
     *   batch: UrgentMails and NonUrgentMails (Both are FIFO)
     *      MailA - MailB - MailC - Mail1 - Mail2 - Mail3 - Mail4 - Mail5 (the first batch)
     *
     * To ensure that urgent mails are executed in FIFO order, moving mails from queue to batch
     * only happens when batch doesn't have urgent mails. For examples, the moving will happen when
     * MailA, MailB and MailC are taken from batch:
     *
     * Step1 : moving all urgent mails into the head of batch:
     *   queue: Mail6 - Mail7 - Mail8
     *   batch: MailE - MailF - MailG - Mail1 - Mail2 - Mail3 - Mail4
     * Step2 : moving all non-urgent mails into the tail of batch:
     *   queue:
     *   batch: MailE - MailF - MailG - Mail1 - Mail2 - Mail3 - Mail4 - Mail5 - Mail6 - Mail7
     * }</pre>
     */
    private void moveUrgentMailsToBatchIfNeeded(boolean onlyMoveUrgentMails) {
        checkIsMailboxThread();
        Mail peek = batch.peek();
        if (peek != null) {
            if (peek.getMailOptions().isUrgent()) {
                // To ensure that urgent mails are executed in FIFO order, moving mails from queue
                // to batch only happens when batch doesn't have urgent mails.
                return;
            } else if (!hasNewUrgentMail) {
                // In order to reduce the frequent lock acquisition caused by movement, if the batch
                // is not empty and there are no new urgent emails, there is no need to move.
                return;
            }
        } else {
            if (onlyMoveUrgentMails && !hasNewUrgentMail) {
                return;
            }
            if (!hasNewMail) {
                // Both batch and queue are empty
                return;
            }
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Mail mail;
            while ((mail = queue.pollFirst()) != null) {
                if (mail.getMailOptions().isUrgent()) {
                    batch.addFirst(mail);
                } else {
                    if (onlyMoveUrgentMails) {
                        // Put non-urgent mail back into the queue, and stop the loop
                        queue.addFirst(mail);
                        break;
                    } else {
                        batch.addLast(mail);
                    }
                }
            }
            hasNewUrgentMail = false;
            hasNewMail = !queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTakeFromBatch() {
        checkIsMailboxThread();
        checkTakeStateConditions();
        moveUrgentMailsToBatchIfNeeded(true);
        return Optional.ofNullable(batch.pollFirst());
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void put(@Nonnull Mail mail) {
        if (mail.getMailOptions().isUrgent()) {
            putFirst(mail);
        } else {
            putLast(mail);
        }
    }

    /** Adds the given action to the tail of the mailbox. */
    private void putLast(@Nonnull Mail mail) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            checkPutStateConditions();
            queue.addLast(mail);
            hasNewMail = true;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Adds the given action to the head of the mailbox. */
    private void putFirst(@Nonnull Mail mail) {
        Mail peek = batch.peek();
        if (isMailboxThread()
                && peek != null
                && !peek.getMailOptions().isUrgent()
                && !hasNewUrgentMail) {
            // To ensure that urgent mails are executed in FIFO order, the urgent mail can be put in
            // batch directly if there is no any urgent mail in mailbox
            checkPutStateConditions();
            batch.addFirst(mail);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                checkPutStateConditions();
                queue.addFirst(mail);
                hasNewMail = true;
                hasNewUrgentMail = true;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Nullable
    private Mail takeOrNull(Deque<Mail> queue, int priority) {
        if (queue.isEmpty()) {
            return null;
        }

        Iterator<Mail> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Mail mail = iterator.next();
            if (mail.getPriority() >= priority) {
                iterator.remove();
                return mail;
            }
        }
        return null;
    }

    @Override
    public List<Mail> drain() {
        List<Mail> drainedMails = new ArrayList<>(batch);
        batch.clear();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            drainedMails.addAll(queue);
            queue.clear();
            hasNewUrgentMail = false;
            hasNewMail = false;
            return drainedMails;
        } finally {
            lock.unlock();
        }
    }

    private void checkIsMailboxThread() {
        if (!isMailboxThread()) {
            throw new IllegalStateException(
                    "Illegal thread detected. This method must be called from inside the mailbox thread!");
        }
    }

    private void checkPutStateConditions() {
        if (state != OPEN) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " for put operations.");
        }
    }

    private void checkTakeStateConditions() {
        if (state == CLOSED) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " or "
                            + QUIESCED
                            + " for take operations.");
        }
    }

    @Override
    public void quiesce() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == OPEN) {
                state = QUIESCED;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Nonnull
    @Override
    public List<Mail> close() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == CLOSED) {
                return Collections.emptyList();
            }
            List<Mail> droppedMails = drain();
            state = CLOSED;
            // to unblock all
            notEmpty.signalAll();
            return droppedMails;
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    @Override
    public State getState() {
        if (isMailboxThread()) {
            return state;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void runExclusively(Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }
}
