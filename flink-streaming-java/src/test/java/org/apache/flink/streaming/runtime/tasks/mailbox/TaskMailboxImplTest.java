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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MAX_PRIORITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TaskMailboxImpl}. */
class TaskMailboxImplTest {

    private static final RunnableWithException NO_OP = () -> {};
    private static final int DEFAULT_PRIORITY = 0;
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
    void testPutAsHead() throws InterruptedException {

        Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
        Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");
        Mail mailC = new Mail(() -> {}, DEFAULT_PRIORITY, "mailC, DEFAULT_PRIORITY");
        Mail mailD = new Mail(() -> {}, DEFAULT_PRIORITY, "mailD, DEFAULT_PRIORITY");

        taskMailbox.put(mailC);
        taskMailbox.putFirst(mailB);
        taskMailbox.put(mailD);
        taskMailbox.putFirst(mailA);

        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailA);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailB);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailC);
        assertThat(taskMailbox.take(DEFAULT_PRIORITY)).isSameAs(mailD);

        assertThat(taskMailbox.tryTake(DEFAULT_PRIORITY)).isNotPresent();
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

        assertThatThrownBy(() -> taskMailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP")))
                .isInstanceOf(MailboxClosedException.class);
    }

    private void testAllPuttingUnblocksInternal(Consumer<TaskMailbox> unblockMethod)
            throws InterruptedException {
        testUnblocksInternal(
                () -> taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY")),
                unblockMethod);
        setUp();
        testUnblocksInternal(
                () -> taskMailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP")), unblockMethod);
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

        Mail mailA = new Mail(() -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        taskMailbox.put(mailC);
        taskMailbox.put(mailB);
        taskMailbox.put(mailD);
        taskMailbox.putFirst(mailA);

        assertThat(taskMailbox.take(2)).isSameAs(mailA);
        assertThat(taskMailbox.take(2)).isSameAs(mailB);
        assertThat(taskMailbox.tryTake(2)).isNotPresent();

        assertThat(taskMailbox.take(1)).isSameAs(mailC);
        assertThat(taskMailbox.take(1)).isSameAs(mailD);

        assertThat(taskMailbox.tryTake(1)).isNotPresent();
    }

    @Test
    void testPutWithPriorityAndReadingFromMainMailbox() throws InterruptedException {

        Mail mailA = new Mail(() -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        taskMailbox.put(mailC);
        taskMailbox.put(mailB);
        taskMailbox.put(mailD);
        taskMailbox.putFirst(mailA);

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
}
