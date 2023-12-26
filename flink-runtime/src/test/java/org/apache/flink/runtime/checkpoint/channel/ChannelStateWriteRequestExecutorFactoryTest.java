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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ChannelStateWriteRequestExecutorFactory} */
public class ChannelStateWriteRequestExecutorFactoryTest {

    private static final CheckpointStorage CHECKPOINT_STORAGE = new JobManagerCheckpointStorage();

    @Test
    void testReuseExecutorForSameJobId() {
        assertReuseExecutor(1);
        assertReuseExecutor(2);
        assertReuseExecutor(3);
        assertReuseExecutor(5);
        assertReuseExecutor(10);
    }

    private void assertReuseExecutor(int maxSubtasksPerChannelStateFile) {
        JobID JOB_ID = new JobID();
        Random RANDOM = new Random();
        ChannelStateWriteRequestExecutorFactory executorFactory =
                new ChannelStateWriteRequestExecutorFactory(JOB_ID);
        int numberOfTasks = 100;

        ChannelStateWriteRequestExecutor currentExecutor = null;
        for (int i = 0; i < numberOfTasks; i++) {
            ChannelStateWriteRequestExecutor newExecutor =
                    executorFactory.getOrCreateExecutor(
                            new JobVertexID(),
                            RANDOM.nextInt(numberOfTasks),
                            CHECKPOINT_STORAGE,
                            maxSubtasksPerChannelStateFile);
            if (i % maxSubtasksPerChannelStateFile == 0) {
                assertThat(newExecutor)
                        .as("Factory should create the new executor.")
                        .isNotSameAs(currentExecutor);
                currentExecutor = newExecutor;
            } else {
                assertThat(newExecutor)
                        .as("Factory should reuse the old executor.")
                        .isSameAs(currentExecutor);
            }
        }
    }

    @Test
    void testSomeSubtasksCloseDuringOtherSubtasksStarting() throws Exception {
        JobID jobID = new JobID();
        JobVertexID jobVertexID = new JobVertexID();
        int numberOfSubtask = 100_000;
        int maxSubtasksPerChannelStateFile = 10;

        ChannelStateWriteRequestExecutorFactory executorFactory =
                new ChannelStateWriteRequestExecutorFactory(jobID);

        BlockingQueue<ChannelStateWriteRequestExecutor> queue = new LinkedBlockingQueue<>(100);

        CompletableFuture<Void> createFuture = new CompletableFuture<>();
        new Thread(
                        () -> {
                            try {
                                for (int i = 0; i < numberOfSubtask; i++) {
                                    ChannelStateWriteRequestExecutor executor =
                                            executorFactory.getOrCreateExecutor(
                                                    jobVertexID,
                                                    i,
                                                    CHECKPOINT_STORAGE,
                                                    maxSubtasksPerChannelStateFile,
                                                    false);
                                    assertThat(executor).isNotNull();
                                    queue.put(executor);
                                }
                                createFuture.complete(null);
                            } catch (Throwable e) {
                                createFuture.completeExceptionally(e);
                            }
                        })
                .start();

        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        new Thread(
                        () -> {
                            try {
                                for (int i = 0; i < numberOfSubtask; i++) {
                                    ChannelStateWriteRequestExecutor executor = queue.take();
                                    executor.releaseSubtask(jobVertexID, numberOfSubtask);
                                }
                                releaseFuture.complete(null);
                            } catch (Throwable e) {
                                releaseFuture.completeExceptionally(e);
                            }
                        })
                .start();

        createFuture.get();
        releaseFuture.get();
    }
}
