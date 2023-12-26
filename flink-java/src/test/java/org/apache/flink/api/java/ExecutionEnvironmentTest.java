/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionEnvironmentTest {

    @Test
    void testConcurrentSetContext() throws Exception {
        int numThreads = 20;
        final CountDownLatch waitingThreadCount = new CountDownLatch(numThreads);
        final OneShotLatch latch = new OneShotLatch();
        final List<CheckedThread> threads = new ArrayList<>();
        for (int x = 0; x < numThreads; x++) {
            final CheckedThread thread =
                    new CheckedThread() {
                        @Override
                        public void go() {
                            final ExecutionEnvironment preparedEnvironment =
                                    new ExecutionEnvironment();
                            ExecutionEnvironment.initializeContextEnvironment(
                                    () -> preparedEnvironment);
                            try {
                                waitingThreadCount.countDown();
                                latch.awaitQuietly();
                                assertThat(ExecutionEnvironment.getExecutionEnvironment())
                                        .isSameAs(preparedEnvironment);
                            } finally {
                                ExecutionEnvironment.resetContextEnvironment();
                            }
                        }
                    };
            thread.start();
            threads.add(thread);
        }

        // wait for all threads to be ready and trigger the job submissions at the same time
        waitingThreadCount.await();
        latch.trigger();

        for (CheckedThread thread : threads) {
            thread.sync();
        }
    }
}
