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
import org.apache.flink.api.common.operators.MailboxExecutor;

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
 *
 * <p>Mails are stored in three containers:
 *
 * <ul>
 *   <li>{@link #urgentQueue}: urgent mails ({@link MailboxExecutor.MailOptions#isUrgent()}) in
 *       arrival order (FIFO), guarded by the lock.
 *   <li>{@link #queue}: all other mails in arrival order, guarded by the lock.
 *   <li>{@link #batch}: a bounded round of normal mails that was moved out of {@link #queue} by
 *       {@link #createBatch()}; accessed by the mailbox thread only and therefore lock-free.
 * </ul>
 *
 * <p>Mails are served in the order {@code urgentQueue}, {@code batch}, {@code queue}. Within each
 * container the oldest eligible mail goes first, so urgent mails of equal priority are FIFO among
 * themselves, no matter which thread puts them or which method takes them; a priority take may skip
 * an older urgent mail whose priority is below the requested one. Urgent mails are the only mails
 * that may be taken from {@link #tryTakeFromBatch()} although they were not part of the batch when
 * it was created.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
    /** Lock for all concurrent ops. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Internal queue of normal (non-urgent) mails. */
    @GuardedBy("lock")
    private final Deque<Mail> queue = new ArrayDeque<>();

    /** Internal queue of urgent mails in arrival order. */
    @GuardedBy("lock")
    private final Deque<Mail> urgentQueue = new ArrayDeque<>();

    /** Condition that is triggered when the mailbox is no longer empty. */
    @GuardedBy("lock")
    private final Condition notEmpty = lock.newCondition();

    /** The state of the mailbox in the lifecycle of open, quiesced, and closed. */
    @GuardedBy("lock")
    private State state = OPEN;

    /** Reference to the thread that executes the mailbox mails. */
    @Nonnull private final Thread taskMailboxThread;

    /**
     * The current batch of normal mails. A new batch can be created with {@link #createBatch()} and
     * consumed with {@link #tryTakeFromBatch()}. Must only be accessed by the mailbox thread
     * (except under the lock in {@link #drain()} and {@link #size()}).
     */
    private final Deque<Mail> batch = new ArrayDeque<>();

    /**
     * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of
     * {@link #batch} or {@link #urgentQueue}.
     */
    private volatile boolean hasNewMail = false;

    /**
     * Performance optimization where hasNewUrgentMail == !urgentQueue.isEmpty(). It is the only
     * additional volatile read on the take path if there are no urgent mails.
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
        return !batch.isEmpty() || hasNewMail || hasNewUrgentMail;
    }

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return batch.size() + queue.size() + urgentQueue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTake(int priority) {
        return tryTake(priority, false);
    }

    @Override
    public Optional<Mail> tryTakeIgnoringDeferrable(int priority) {
        return tryTake(priority, true);
    }

    private Optional<Mail> tryTake(int priority, boolean ignoreDeferrable) {
        checkIsMailboxThread();
        checkTakeStateConditions();

        Mail urgentHead = takeUrgentIfPending(priority, ignoreDeferrable);
        if (urgentHead != null) {
            return Optional.of(urgentHead);
        }
        Mail head = takeOrNull(batch, priority, ignoreDeferrable);
        if (head != null) {
            return Optional.of(head);
        }
        if (!hasNewMail) {
            return Optional.empty();
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Mail urgent = takeUrgentLocked(priority, ignoreDeferrable);
            if (urgent != null) {
                return Optional.of(urgent);
            }
            return Optional.ofNullable(takeNormalLocked(priority, ignoreDeferrable));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public @Nonnull Mail take(int priority) throws InterruptedException, IllegalStateException {
        checkIsMailboxThread();
        checkTakeStateConditions();

        Mail urgentHead = takeUrgentIfPending(priority, false);
        if (urgentHead != null) {
            return urgentHead;
        }
        Mail head = takeOrNull(batch, priority, false);
        if (head != null) {
            return head;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (true) {
                Mail urgent = takeUrgentLocked(priority, false);
                if (urgent != null) {
                    return urgent;
                }
                Mail mail = takeNormalLocked(priority, false);
                if (mail != null) {
                    return mail;
                }
                // to ease debugging
                notEmpty.await(1, TimeUnit.SECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean createBatch() {
        checkIsMailboxThread();
        if (hasNewMail) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                batch.addAll(queue);
                queue.clear();
                hasNewMail = false;
            } finally {
                lock.unlock();
            }
        }
        // pending urgent mails are served by tryTakeFromBatch, too
        return !batch.isEmpty() || hasNewUrgentMail;
    }

    @Override
    public Optional<Mail> tryTakeFromBatch() {
        checkIsMailboxThread();
        checkTakeStateConditions();
        Mail urgent = takeUrgentIfPending(Integer.MIN_VALUE, false);
        if (urgent != null) {
            return Optional.of(urgent);
        }
        return Optional.ofNullable(batch.pollFirst());
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void put(@Nonnull Mail mail) {
        final boolean urgent = mail.getMailOptions().isUrgent();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            checkPutStateConditions();
            if (urgent) {
                final boolean wasEmpty = urgentQueue.isEmpty();
                urgentQueue.addLast(mail);
                if (wasEmpty) {
                    hasNewUrgentMail = true;
                }
            } else {
                final boolean wasEmpty = queue.isEmpty();
                queue.addLast(mail);
                if (wasEmpty) {
                    hasNewMail = true;
                }
            }
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    /**
     * Takes the oldest eligible urgent mail, if any, without acquiring the lock when no urgent mail
     * is pending according to the volatile {@link #hasNewUrgentMail}.
     */
    @Nullable
    private Mail takeUrgentIfPending(int priority, boolean ignoreDeferrable) {
        return hasNewUrgentMail ? takeUrgentOrNull(priority, ignoreDeferrable) : null;
    }

    @Nullable
    private Mail takeUrgentOrNull(int priority, boolean ignoreDeferrable) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return takeUrgentLocked(priority, ignoreDeferrable);
        } finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock")
    @Nullable
    private Mail takeUrgentLocked(int priority, boolean ignoreDeferrable) {
        final Mail mail = takeOrNull(urgentQueue, priority, ignoreDeferrable);
        if (mail != null && urgentQueue.isEmpty()) {
            hasNewUrgentMail = false;
        }
        return mail;
    }

    @GuardedBy("lock")
    @Nullable
    private Mail takeNormalLocked(int priority, boolean ignoreDeferrable) {
        final Mail mail = takeOrNull(queue, priority, ignoreDeferrable);
        if (mail != null && queue.isEmpty()) {
            hasNewMail = false;
        }
        return mail;
    }

    @Nullable
    private Mail takeOrNull(Deque<Mail> queue, int priority, boolean ignoreDeferrable) {
        if (queue.isEmpty()) {
            return null;
        }

        Iterator<Mail> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Mail mail = iterator.next();
            int mailPriority =
                    ignoreDeferrable ? mail.getPriorityIgnoringDeferrable() : mail.getPriority();
            if (mailPriority >= priority) {
                iterator.remove();
                return mail;
            }
        }
        return null;
    }

    /** Drains all mails in the order in which they would be taken: urgent, batch, normal. */
    @Override
    public List<Mail> drain() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            List<Mail> drainedMails =
                    new ArrayList<>(urgentQueue.size() + batch.size() + queue.size());
            drainedMails.addAll(urgentQueue);
            drainedMails.addAll(batch);
            drainedMails.addAll(queue);
            urgentQueue.clear();
            batch.clear();
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
