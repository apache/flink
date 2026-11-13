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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MailboxClosedException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.api.common.operators.MailboxExecutor.MailOptions.urgent;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MAX_PRIORITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TaskMailboxImpl}. */
class TaskMailboxImplTest {

    private static final RunnableWithException NO_OP = () -> {};
    private static final int DEFAULT_PRIORITY = 0;
    private static final int HIGH_PRIORITY = 1;
    private static final long TIMEOUT_SECONDS = 60;

    /** Object under test. */
    private TaskMailbox taskMailbox;

    @BeforeEach
    void setUp() {
        taskMailbox = new TaskMailboxImpl();
    }

    @AfterEach
    void tearDown() {
        taskMailbox.close();
    }

    @Test
    void testContracts() throws InterruptedException {
        final Queue<Mail> testObjects = new LinkedList<>();
        assertThat(taskMailbox.hasMail()).isFalse();

        for (int i = 0; i < 10; ++i) {
            final Mail mail = new Mail(NO_OP, DEFAULT_PRIORITY, "mail, DEFAULT_PRIORITY");
            testObjects.add(mail);
            taskMailbox.put(mail);
            assertThat(taskMailbox.hasMail()).isTrue();
        }

        while (!testObjects.isEmpty()) {
            assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(testObjects.remove());
            assertThat(taskMailbox.hasMail()).isEqualTo(!testObjects.isEmpty());
        }
    }

    /** Test the producer-consumer pattern using the blocking methods on the mailbox. */
    @Test
    void testConcurrentPutTakeBlocking() throws Exception {
        testPutTake(mailbox -> mailbox.take(DEFAULT_PRIORITY));
    }

    /** Test the producer-consumer pattern using the non-blocking methods & waits on the mailbox. */
    @Test
    void testConcurrentPutTakeNonBlockingAndWait() throws Exception {
        testPutTake(
                (mailbox -> {
                    Optional<Mail> optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
                    while (!optionalMail.isPresent()) {
                        optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
                    }
                    return optionalMail.get();
                }));
    }

    /** Test that closing the mailbox unblocks pending accesses with correct exceptions. */
    @Test
    void testCloseUnblocks() throws InterruptedException {
        testAllPuttingUnblocksInternal(TaskMailbox::close);
    }

    /** Test that silencing the mailbox unblocks pending accesses with correct exceptions. */
    @Test
    void testQuiesceUnblocks() throws InterruptedException {
        testAllPuttingUnblocksInternal(TaskMailbox::quiesce);
    }

    @Test
    void testLifeCycleQuiesce() throws InterruptedException {
        taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
        taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
        taskMailbox.quiesce();
        testLifecyclePuttingInternal();
        taskMailbox.take(DEFAULT_PRIORITY);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).isPresent();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).isNotPresent();
    }

    @Test
    void testLifeCycleClose() {
        taskMailbox.close();
        testLifecyclePuttingInternal();

        assertThatThrownBy(() -> taskMailbox.take(DEFAULT_PRIORITY))
                .isInstanceOf(MailboxClosedException.class);

        assertThatThrownBy(() -> taskMailbox.tryTake(DEFAULT_PRIORITY))
                .isInstanceOf(MailboxClosedException.class);
    }

    private void testLifecyclePuttingInternal() {
        assertThatThrownBy(
                        () ->
                                taskMailbox.put(
                                        new Mail(
                                                NO_OP,
                                                DEFAULT_PRIORITY,
                                                "NO_OP, DEFAULT_PRIORITY")))
                .isInstanceOf(MailboxClosedException.class);

        assertThatThrownBy(() -> taskMailbox.put(new Mail(urgent(), NO_OP, MAX_PRIORITY, "NO_OP")))
                .isInstanceOf(MailboxClosedException.class);
    }

    private void testAllPuttingUnblocksInternal(Consumer<TaskMailbox> unblockMethod)
            throws InterruptedException {
        testUnblocksInternal(
                () -> taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY")),
                unblockMethod);
        setUp();
        testUnblocksInternal(
                () -> taskMailbox.put(new Mail(urgent(), NO_OP, MAX_PRIORITY, "NO_OP")),
                unblockMethod);
    }

    private void testUnblocksInternal(
            RunnableWithException testMethod, Consumer<TaskMailbox> unblockMethod)
            throws InterruptedException {
        final CheckedThread[] blockedThreads = new CheckedThread[8];

        CountDownLatch countDownLatch = new CountDownLatch(blockedThreads.length);

        for (int i = 0; i < blockedThreads.length; ++i) {
            CheckedThread blocked =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            countDownLatch.countDown();
                            while (true) {
                                testMethod.run();
                            }
                        }
                    };
            blockedThreads[i] = blocked;
            blocked.start();
        }

        countDownLatch.await();
        unblockMethod.accept(taskMailbox);

        for (CheckedThread blockedThread : blockedThreads) {
            assertThatThrownBy(blockedThread::sync).isInstanceOf(MailboxClosedException.class);
        }
    }

    /**
     * Test producer-consumer pattern through the mailbox in a concurrent setting (n-writer /
     * 1-reader).
     */
    private void testPutTake(
            FunctionWithException<TaskMailbox, Mail, InterruptedException> takeMethod)
            throws Exception {
        final int numThreads = 10;
        final int numMailsPerThread = 1000;
        final int[] results = new int[numThreads];
        Thread[] writerThreads = new Thread[numThreads];

        for (int i = 0; i < writerThreads.length; ++i) {
            final int threadId = i;
            writerThreads[i] =
                    new Thread(
                            ThrowingRunnable.unchecked(
                                    () -> {
                                        for (int k = 0; k < numMailsPerThread; ++k) {
                                            taskMailbox.put(
                                                    new Mail(
                                                            () -> ++results[threadId],
                                                            DEFAULT_PRIORITY,
                                                            "result " + k));
                                        }
                                    }));
        }

        for (Thread writerThread : writerThreads) {
            writerThread.start();
        }

        for (Thread writerThread : writerThreads) {
            writerThread.join();
        }

        AtomicBoolean isRunning = new AtomicBoolean(true);
        taskMailbox.put(
                new Mail(
                        () -> isRunning.set(false),
                        DEFAULT_PRIORITY,
                        "POISON_MAIL, DEFAULT_PRIORITY"));

        while (isRunning.get()) {
            takeMethod.apply(taskMailbox).run();
        }
        for (int perThreadResult : results) {
            assertThat(perThreadResult).isEqualTo(numMailsPerThread);
        }
    }

    @Test
    void testPutAsHeadWithPriority() throws InterruptedException {

        Mail mailA = new Mail(urgent(), () -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        taskMailbox.put(mailC);
        taskMailbox.put(mailB);
        taskMailbox.put(mailD);
        taskMailbox.put(mailA);

        assertThat(taskMailbox.take(2)).isSameAs(mailA);
        assertThat(taskMailbox.take(2)).isSameAs(mailB);
        assertThat(taskMailbox.tryTake(2)).isNotPresent();

        assertThat(taskMailbox.take(1)).isSameAs(mailC);
        assertThat(taskMailbox.take(1)).isSameAs(mailD);

        assertThat(taskMailbox.tryTake(1)).isNotPresent();
    }

    @Test
    void testPutWithPriorityAndReadingFromMainMailbox() throws InterruptedException {

        Mail mailA = new Mail(urgent(), () -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        taskMailbox.put(mailC);
        taskMailbox.put(mailB);
        taskMailbox.put(mailD);
        taskMailbox.put(mailA);

        // same order for non-priority and priority on top
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailA);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailC);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailB);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailD);
    }

    /**
     * Tests the interaction of batch and non-batch methods.
     *
     * <p>Both {@link TaskMailbox#take(int)} and {@link TaskMailbox#tryTake(int)} consume the batch
     * but once drained will fetch elements from the remaining mails.
     *
     * <p>In contrast, {@link TaskMailbox#tryTakeFromBatch()} will not return any mail once the
     * batch is drained.
     */
    @Test
    void testBatchAndNonBatchTake() throws InterruptedException {
        final List<Mail> mails =
                IntStream.range(0, 6)
                        .mapToObj(i -> new Mail(NO_OP, DEFAULT_PRIORITY, String.valueOf(i)))
                        .collect(Collectors.toList());

        // create a batch with 3 mails
        mails.subList(0, 3).forEach(taskMailbox::put);
        assertThat(taskMailbox.createBatch()).isTrue();
        // add 3 more mails after the batch
        mails.subList(3, 6).forEach(taskMailbox::put);

        // now take all mails in the batch with all available methods
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mails.get(0));
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mails.get(1));
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mails.get(2));

        // batch empty, so only regular methods work
        assertThat(taskMailbox.tryTakeFromBatch()).isEmpty();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mails.get(3));
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mails.get(4));

        // one unprocessed mail left
        assertThat(taskMailbox.close()).containsExactly(mails.get(5));
    }

    @Test
    void testBatchDrain() {

        Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
        Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");

        taskMailbox.put(mailA);
        assertThat(taskMailbox.createBatch()).isTrue();
        taskMailbox.put(mailB);

        assertThat(taskMailbox.drain()).containsExactly(mailA, mailB);
    }

    /** Drained mails must come in the order they would be taken, as they may be executed. */
    @Test
    void testDrainOrderMatchesTakeOrder() throws Exception {
        // vary the number of urgent and of normal mails in the batch; then urgent and normal mails
        // arrive in the queue
        for (int batchUrgent = 0; batchUrgent <= 2; batchUrgent++) {
            for (int batchNormal = 0; batchNormal <= 2; batchNormal++) {
                final int urgentInBatch = batchUrgent;
                final int normalInBatch = batchNormal;
                TaskMailbox takeMailbox = new TaskMailboxImpl();
                fillForDrainTest(takeMailbox, urgentInBatch, normalInBatch);
                List<Mail> taken = new ArrayList<>();
                while (takeMailbox.hasMail()) {
                    taken.add(takeMailbox.take(DEFAULT_PRIORITY));
                }

                TaskMailbox drainMailbox = new TaskMailboxImpl();
                fillForDrainTest(drainMailbox, urgentInBatch, normalInBatch);
                List<Mail> drained = drainMailbox.drain();

                assertThat(drained)
                        .as("urgentInBatch=%d, normalInBatch=%d", urgentInBatch, normalInBatch)
                        .extracting(Mail::toString)
                        .containsExactlyElementsOf(
                                taken.stream().map(Mail::toString).collect(Collectors.toList()));
                assertThat(drainMailbox.hasMail()).isFalse();
                assertThat(drainMailbox.size()).isZero();
                takeMailbox.close();
                drainMailbox.close();
            }
        }
    }

    /**
     * Creates a mailbox state with the given number of normal mails in the batch, optionally urgent
     * mails put by the mailbox thread (which end up at the batch head or in the queue), and urgent
     * mails A, B and normal mails put from another thread into the queue. Descriptions are unique
     * so that the mails of different mailboxes can be compared by their description.
     */
    private static void fillForDrainTest(
            TaskMailbox mailbox, int urgentInBatch, int normalInBatch) {
        for (int i = 0; i < normalInBatch; i++) {
            mailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "batch normal %d", i));
        }
        if (normalInBatch > 0) {
            mailbox.createBatch();
        }
        for (int i = 0; i < urgentInBatch; i++) {
            mailbox.put(new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "urgent %d", i));
        }
        if (normalInBatch == 0 && urgentInBatch > 0) {
            mailbox.createBatch();
        }

        putFromOtherThread(mailbox, new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "queue urgent A"));
        putFromOtherThread(mailbox, new Mail(NO_OP, DEFAULT_PRIORITY, "queue normal 1"));
        putFromOtherThread(mailbox, new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "queue urgent B"));
        putFromOtherThread(mailbox, new Mail(NO_OP, DEFAULT_PRIORITY, "queue normal 2"));
    }

    @Test
    void testDrainReturnsQueuedUrgentMailsInArrivalOrder() {
        Mail urgentA = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "urgent A");
        Mail urgentB = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "urgent B");
        Mail normal = new Mail(NO_OP, DEFAULT_PRIORITY, "normal");
        Mail batched = new Mail(NO_OP, DEFAULT_PRIORITY, "batched");

        taskMailbox.put(batched);
        taskMailbox.createBatch();
        putFromOtherThread(taskMailbox, normal);
        putFromOtherThread(taskMailbox, urgentA);
        putFromOtherThread(taskMailbox, urgentB);

        assertThat(taskMailbox.drain()).containsExactly(urgentA, urgentB, batched, normal);
    }

    private static void putFromOtherThread(TaskMailbox mailbox, Mail mail) {
        Thread thread = new Thread(() -> mailbox.put(mail));
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * An urgent mail that is not takeable at the requested priority must not affect the order of
     * the takeable urgent mails that arrive after it: they are returned in arrival order.
     */
    @Test
    void testTryTakeWithPriorityKeepsUrgentMailsFifo() {
        putUntakeableUrgentMail();
        Mail urgentA = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent A");
        Mail urgentB = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent B");
        Mail urgentC = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent C");
        putFromOtherThread(taskMailbox, urgentA);
        putFromOtherThread(taskMailbox, urgentB);
        putFromOtherThread(taskMailbox, urgentC);

        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(urgentA);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(urgentB);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(urgentC);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isNotPresent();
    }

    @Test
    void testTryTakeIgnoringDeferrableKeepsUrgentMailsFifo() {
        putUntakeableUrgentMail();
        Mail urgentA = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent A");
        Mail urgentB = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent B");
        putFromOtherThread(taskMailbox, urgentA);
        putFromOtherThread(taskMailbox, urgentB);

        assertThat(taskMailbox.tryTakeIgnoringDeferrable(HIGH_PRIORITY)).contains(urgentA);
        assertThat(taskMailbox.tryTakeIgnoringDeferrable(HIGH_PRIORITY)).contains(urgentB);
    }

    @Test
    void testBlockingTakeWithPriorityKeepsUrgentMailsFifo() throws Exception {
        putUntakeableUrgentMail();
        Mail urgentA = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent A");
        Mail urgentB = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent B");
        putFromOtherThread(taskMailbox, urgentA);
        putFromOtherThread(taskMailbox, urgentB);

        assertThat(taskMailbox.take(HIGH_PRIORITY)).isSameAs(urgentA);
        assertThat(taskMailbox.take(HIGH_PRIORITY)).isSameAs(urgentB);
    }

    /**
     * A takeable urgent mail must be returned before a takeable normal mail, even if an untakeable
     * urgent mail and the takeable normal mail precede it.
     */
    @Test
    void testTakeableUrgentMailOvertakesUntakeableUrgentAndTakeableNormalMail() throws Exception {
        Mail untakeableUrgent = putUntakeableUrgentMail();
        Mail normal = new Mail(NO_OP, HIGH_PRIORITY, "normal");
        Mail takeableUrgent = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "takeable urgent");
        putFromOtherThread(taskMailbox, normal);
        putFromOtherThread(taskMailbox, takeableUrgent);

        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(takeableUrgent);
        assertThat(taskMailbox.take(HIGH_PRIORITY)).isSameAs(normal);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isNotPresent();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(untakeableUrgent);
    }

    /**
     * Urgent mails that arrive after the mailbox thread has checked for pending urgent mails, but
     * before it polls the normal queue, must still be returned before the queued normal mails. The
     * interleaving is made deterministic by a batch mail that puts the mails when it is inspected.
     */
    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void testUrgentMailArrivingDuringTryTakeIsNotOvertakenByNormalMails(boolean ignoreDeferrable) {
        Mail queuedNormal = new Mail(NO_OP, HIGH_PRIORITY, "queued normal");
        Mail urgentMail = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent");
        Mail lateNormal = new Mail(NO_OP, HIGH_PRIORITY, "late normal");
        AtomicBoolean injected = new AtomicBoolean();
        Runnable injectLateMails =
                () -> {
                    if (injected.compareAndSet(false, true)) {
                        putFromOtherThread(taskMailbox, urgentMail);
                        putFromOtherThread(taskMailbox, lateNormal);
                    }
                };
        // an untakeable batch mail, which is inspected after the check for urgent mails
        taskMailbox.put(new InspectionHookMail(injectLateMails));
        assertThat(taskMailbox.createBatch()).isTrue();
        taskMailbox.put(queuedNormal);

        Optional<Mail> taken =
                ignoreDeferrable
                        ? taskMailbox.tryTakeIgnoringDeferrable(HIGH_PRIORITY)
                        : taskMailbox.tryTake(HIGH_PRIORITY);

        assertThat(injected).isTrue();
        assertThat(taken).contains(urgentMail);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(queuedNormal);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(lateNormal);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isNotPresent();
    }

    /** An untakeable mail that runs the given hook the first time its priority is inspected. */
    private static class InspectionHookMail extends Mail {
        private final Runnable hook;

        InspectionHookMail(Runnable hook) {
            super(NO_OP, DEFAULT_PRIORITY, "inspection hook");
            this.hook = hook;
        }

        @Override
        public int getPriority() {
            hook.run();
            return super.getPriority();
        }

        @Override
        public int getPriorityIgnoringDeferrable() {
            hook.run();
            return super.getPriorityIgnoringDeferrable();
        }
    }

    /** A pending urgent mail alone makes the batch available and is served by the batch take. */
    @Test
    void testUrgentMailIsServedFromEmptyBatch() {
        Mail lowPriorityUrgent = new Mail(urgent(), NO_OP, TaskMailbox.MIN_PRIORITY, "urgent");
        assertThat(taskMailbox.createBatch()).isFalse();
        putFromOtherThread(taskMailbox, lowPriorityUrgent);

        assertThat(taskMailbox.hasMail()).isTrue();
        assertThat(taskMailbox.size()).isOne();
        assertThat(taskMailbox.createBatch()).isTrue();
        assertThat(taskMailbox.tryTakeFromBatch()).contains(lowPriorityUrgent);
        assertThat(taskMailbox.tryTakeFromBatch()).isNotPresent();
        assertThat(taskMailbox.hasMail()).isFalse();
        assertThat(taskMailbox.size()).isZero();
    }

    /** Untakeable urgent mails are skipped and normal mails are taken only after urgent ones. */
    @Test
    void testTakeWithPriorityMixedUrgentAndNormalMails() {
        Mail untakeableHead = putUntakeableUrgentMail();
        Mail normal1 = new Mail(NO_OP, HIGH_PRIORITY, "normal 1");
        Mail untakeableNormal = new Mail(NO_OP, DEFAULT_PRIORITY, "untakeable normal");
        Mail urgentA = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent A");
        Mail untakeableUrgent = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "untakeable urgent");
        Mail urgentB = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "urgent B");
        Mail normal2 = new Mail(NO_OP, HIGH_PRIORITY, "normal 2");
        for (Mail mail :
                Arrays.asList(
                        normal1, untakeableNormal, urgentA, untakeableUrgent, urgentB, normal2)) {
            putFromOtherThread(taskMailbox, mail);
        }

        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(urgentA);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(urgentB);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(normal1);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(normal2);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isNotPresent();
        // the untakeable mails are still there, and the queue flags stay consistent
        assertThat(taskMailbox.size()).isEqualTo(3);
        assertThat(taskMailbox.hasMail()).isTrue();
        // urgent mails first in arrival order, then the normal mail
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(untakeableHead);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(untakeableUrgent);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(untakeableNormal);
        assertThat(taskMailbox.hasMail()).isFalse();
    }

    /**
     * Puts an urgent mail that is not takeable at {@link #HIGH_PRIORITY}, and creates a batch, so
     * that taking has to look past it.
     */
    private Mail putUntakeableUrgentMail() {
        Mail head = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "untakeable urgent head");
        taskMailbox.put(head);
        assertThat(taskMailbox.createBatch()).isTrue();
        return head;
    }

    /** Takes that miss or leave mails behind must keep {@code hasMail()} and later takes right. */
    @Test
    void testFlagsStayConsistentAfterMissingAndPartialTakes() {
        Mail lowUrgent = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "low urgent");
        Mail lowNormal = new Mail(NO_OP, DEFAULT_PRIORITY, "low normal");
        taskMailbox.put(lowUrgent);
        taskMailbox.put(lowNormal);

        // misses on both queues
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isEmpty();
        assertThat(taskMailbox.hasMail()).isTrue();
        assertThat(taskMailbox.size()).isEqualTo(2);

        // takes that leave a (non-takeable) mail behind in both queues
        Mail highUrgent = new Mail(urgent(), NO_OP, HIGH_PRIORITY, "high urgent");
        Mail highNormal = new Mail(NO_OP, HIGH_PRIORITY, "high normal");
        taskMailbox.put(highUrgent);
        taskMailbox.put(highNormal);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(highUrgent);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).contains(highNormal);
        assertThat(taskMailbox.tryTake(HIGH_PRIORITY)).isEmpty();
        assertThat(taskMailbox.hasMail()).isTrue();
        assertThat(taskMailbox.size()).isEqualTo(2);

        // takes that empty the queues
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(lowUrgent);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(lowNormal);
        assertThat(taskMailbox.hasMail()).isFalse();
        assertThat(taskMailbox.size()).isZero();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).isEmpty();

        // the flags are set again by new puts
        Mail again = new Mail(urgent(), NO_OP, DEFAULT_PRIORITY, "again");
        taskMailbox.put(again);
        assertThat(taskMailbox.hasMail()).isTrue();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).contains(again);
        assertThat(taskMailbox.hasMail()).isFalse();
    }

    @Test
    void testBatchPriority() throws Exception {

        Mail mailA = new Mail(() -> {}, 1, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");

        taskMailbox.put(mailA);
        assertThat(taskMailbox.createBatch()).isTrue();
        taskMailbox.put(mailB);

        assertThat(taskMailbox.take(2)).isEqualTo(mailB);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailA);
    }

    /** Testing that we cannot close while running exclusively. */
    @Test
    void testRunExclusively() throws InterruptedException {
        CountDownLatch exclusiveCodeStarted = new CountDownLatch(1);

        final int numMails = 10;

        // send 10 mails in an atomic operation
        new Thread(
                        () ->
                                taskMailbox.runExclusively(
                                        () -> {
                                            exclusiveCodeStarted.countDown();
                                            for (int index = 0; index < numMails; index++) {
                                                try {
                                                    taskMailbox.put(new Mail(() -> {}, 1, "mailD"));
                                                    Thread.sleep(1);
                                                } catch (Exception e) {
                                                }
                                            }
                                        }))
                .start();

        exclusiveCodeStarted.await();
        // make sure that all 10 messages have been actually enqueued.
        assertThat(taskMailbox.close()).hasSize(numMails);
    }

    /** We expect the urgent mail can be taken immediately during taking mail from batch queue. */
    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void testPutHighPriorityMailDuringTakingFromBatch(boolean isAsyncPut) throws Exception {
        Mail mailA = new Mail(() -> {}, DEFAULT_PRIORITY, "mailA");
        Mail mailB = new Mail(() -> {}, DEFAULT_PRIORITY, "mailB");
        Mail mailC = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailC");

        taskMailbox.put(mailA);
        taskMailbox.put(mailB);
        assertThat(taskMailbox.createBatch()).isTrue();
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailA);

        putMail(mailC, isAsyncPut);

        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailC);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailB);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void testMailOrderWithBatch(boolean isAsyncPut) throws Exception {
        Mail mailA = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailA");
        Mail mailB = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailB");
        Mail mailC = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailC");
        Mail mailD = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailD");
        Mail mailE = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailE");
        Mail mailF = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailF");
        Mail mailG = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailG");

        Mail mail1 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail1");
        Mail mail2 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail2");
        Mail mail3 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail3");
        Mail mail4 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail4");
        Mail mail5 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail5");
        Mail mail6 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail6");
        Mail mail7 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail7");

        putMail(mail1, isAsyncPut);
        putMail(mail2, isAsyncPut);
        putMail(mail3, isAsyncPut);
        putMail(mail4, isAsyncPut);
        assertThat(taskMailbox.createBatch()).isTrue();
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail1);

        putMail(mailA, isAsyncPut);
        putMail(mailB, isAsyncPut);
        putMail(mailC, isAsyncPut);
        // All urgent mails should be taken first, including all types of take methods
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailA);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mailB);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mailC);

        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail2);

        putMail(mail5, isAsyncPut);
        putMail(mail6, isAsyncPut);
        putMail(mail7, isAsyncPut);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail3);

        putMail(mailD, isAsyncPut);
        putMail(mailE, isAsyncPut);
        assertThat(taskMailbox.createBatch()).isTrue();
        putMail(mailF, isAsyncPut);
        putMail(mailG, isAsyncPut);

        // All urgent mails should be taken first, including all types of take methods
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailD);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mailE);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mailF);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailG);

        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail4);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mail5);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mail6);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mail7);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void testOnlyOneUrgentMailAtTheSameTime(boolean isAsyncPut) throws Exception {
        Mail mailA = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailA");
        Mail mailB = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailB");
        Mail mailC = new Mail(urgent(), () -> {}, DEFAULT_PRIORITY, "mailC");

        Mail mail1 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail1");
        Mail mail2 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail2");
        Mail mail3 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail3");
        Mail mail4 = new Mail(() -> {}, DEFAULT_PRIORITY, "mail4");

        putMail(mail1, isAsyncPut);
        putMail(mail2, isAsyncPut);
        putMail(mail3, isAsyncPut);
        assertThat(taskMailbox.tryTakeFromBatch()).isEmpty();
        assertThat(taskMailbox.createBatch()).isTrue();
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail1);

        // No existing urgent mail when put a new urgent mail
        putMail(mailA, isAsyncPut);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mailA);
        putMail(mailB, isAsyncPut);
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mailB);
        putMail(mailC, isAsyncPut);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isEqualTo(mailC);

        putMail(mail4, isAsyncPut);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail2);
        assertThat(taskMailbox.tryTakeFromBatch()).hasValue(mail3);
        assertThat(taskMailbox.tryTakeFromBatch()).isEmpty();
        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).hasValue(mail4);
    }

    /**
     * Urgent mails put while the mailbox thread is blocked in {@link TaskMailbox#take(int)} must
     * still be taken in FIFO order, even if the second one is put right after the wake-up caused by
     * the first one.
     *
     * <p>The test is probabilistic by nature (it targets that interleaving); it never fails with a
     * correct implementation, while an implementation that stores urgent mails newest-first fails
     * with high probability.
     */
    @Test
    void testBlockedTakeKeepsUrgentMailsFifo() throws Exception {
        final int pairs = 20_000;
        // Plain list is safe: it is only written by the mailbox thread and read by the test thread
        // after mailboxThread.sync() (join), which establishes happens-before.
        final List<Integer> executed = new ArrayList<>();
        final AtomicReference<TaskMailbox> mailboxRef = new AtomicReference<>();
        final CountDownLatch mailboxCreated = new CountDownLatch(1);
        final Semaphore pairDone = new Semaphore(0);

        CheckedThread mailboxThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        TaskMailbox mailbox = new TaskMailboxImpl(Thread.currentThread());
                        mailboxRef.set(mailbox);
                        mailboxCreated.countDown();
                        for (int i = 0; i < pairs * 2; i++) {
                            mailbox.take(DEFAULT_PRIORITY).run();
                        }
                    }
                };
        mailboxThread.setDaemon(true);
        mailboxThread.start();
        try {
            assertThat(mailboxCreated.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            TaskMailbox mailbox = mailboxRef.get();

            for (int i = 0; i < pairs; i++) {
                final int first = 2 * i;
                final int second = 2 * i + 1;
                mailbox.put(
                        new Mail(
                                urgent(),
                                () -> executed.add(first),
                                DEFAULT_PRIORITY,
                                "first %d",
                                first));
                mailbox.put(
                        new Mail(
                                urgent(),
                                () -> {
                                    executed.add(second);
                                    pairDone.release();
                                },
                                DEFAULT_PRIORITY,
                                "second %d",
                                second));
                assertThat(pairDone.tryAcquire(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .as("mailbox thread did not execute pair %d", i)
                        .isTrue();
            }
        } finally {
            // unblocks a mailbox thread still stuck in take() (it is interruptible) if the test
            // failed early; no-op if the thread already finished
            mailboxThread.interrupt();
            mailboxThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        }
        // rethrows any exception of the mailbox thread
        mailboxThread.sync();

        assertThat(executed)
                .containsExactlyElementsOf(
                        IntStream.range(0, pairs * 2).boxed().collect(Collectors.toList()));
    }

    private void putMail(Mail mail, boolean isAsyncPut) throws Exception {
        if (isAsyncPut) {
            Thread thread = new Thread(() -> taskMailbox.put(mail));
            thread.start();
            thread.join();
        } else {
            taskMailbox.put(mail);
        }
    }
}
