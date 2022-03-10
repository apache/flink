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

package org.apache.flink.changelog.fs;

import org.apache.flink.changelog.fs.BatchingStateChangeUploadSchedulerTest.BlockingUploader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.changelog.inmemory.StateChangelogStorageTest;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.changelog.fs.UnregisteredChangelogStorageMetricGroup.createUnregisteredChangelogStorageMetricGroup;

/** {@link FsStateChangelogStorage} test. */
@RunWith(Parameterized.class)
public class FsStateChangelogStorageTest extends StateChangelogStorageTest {
    @Parameterized.Parameter public boolean compression;

    @Parameterized.Parameters(name = "use compression = {0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    @Override
    protected StateChangelogStorage<?> getFactory() throws IOException {
        return new FsStateChangelogStorage(
                Path.fromLocalFile(temporaryFolder.newFolder()),
                compression,
                1024 * 1024 * 10,
                createUnregisteredChangelogStorageMetricGroup());
    }

    /**
     * Provoke a deadlock between task and uploader threads which might happen during waiting for
     * capacity and upload completion.
     */
    @Test
    public void testDeadlockOnUploadCompletion() throws Throwable {
        int capacity = 10; // in bytes, allow the first two uploads without waiting (see below)
        CountDownLatch remainingUploads = new CountDownLatch(3);
        BlockingUploader blockingUploader = new BlockingUploader();
        CompletableFuture<Void> unblockFuture = new CompletableFuture<>();
        new Thread(
                        () -> {
                            try {
                                remainingUploads.await();
                                blockingUploader.unblock();
                                unblockFuture.complete(null);
                            } catch (Throwable e) {
                                unblockFuture.completeExceptionally(e);
                            }
                        })
                .start();
        MailboxExecutorImpl mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);
        try (BatchingStateChangeUploadScheduler scheduler =
                        new BatchingStateChangeUploadScheduler(
                                0, // schedule immediately
                                0, // schedule immediately
                                RetryPolicy.NONE,
                                blockingUploader,
                                1,
                                capacity,
                                createUnregisteredChangelogStorageMetricGroup()) {
                            @Override
                            public void upload(UploadTask uploadTask) throws IOException {
                                remainingUploads.countDown();
                                super.upload(uploadTask);
                            }
                        };
                StateChangelogWriter<?> writer =
                        new FsStateChangelogStorage(scheduler, 0 /* persist immediately */)
                                .createWriter(
                                        new OperatorID().toString(),
                                        KeyGroupRange.of(0, 0),
                                        mailboxExecutor); ) {
            // 1. start with 1-byte request - releasing only it will NOT allow proceeding in 3, but
            // still involves completion callback, which can deadlock
            writer.append(0, new byte[1]);
            // 2. exceed capacity
            writer.append(0, new byte[capacity]);
            // 3. current thread will block until both previous requests are completed
            // verify that completion can proceed while this thread is waiting
            writer.append(0, new byte[1]);
        }
        // check unblocking thread exit status
        unblockFuture.join();
    }
}
