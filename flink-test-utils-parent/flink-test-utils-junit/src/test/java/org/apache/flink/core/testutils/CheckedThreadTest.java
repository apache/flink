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

package org.apache.flink.core.testutils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing for {@link CheckedThread}. */
class CheckedThreadTest {

    @Test
    void testRunnableThrowException() {
        CheckedThread checkedThread =
                new CheckedThread(
                        () -> {
                            throw new IOException("excepted exception.");
                        });
        checkedThread.start();

        assertThatThrownBy(checkedThread::sync)
                .isInstanceOf(IOException.class)
                .hasMessage("excepted exception.");
    }

    @Test
    void testNullRunnableIsPassed() {
        assertThatThrownBy(() -> new CheckedThread(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CheckedThread(null, "null runnable thread"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @Timeout(10)
    void testSync() {
        CheckedThread checkedThread = new CheckedThread(() -> Thread.sleep(500));

        checkedThread.start();

        assertThatNoException().isThrownBy(checkedThread::sync);
    }

    @Test
    void testSyncWithTimeout() {
        CompletableFuture<Void> blockFuture = new CompletableFuture<>();
        // this thread will be blocked forever.
        CheckedThread checkedThread = new CheckedThread(blockFuture::get);

        checkedThread.start();

        assertThatThrownBy(() -> checkedThread.sync(500)).isInstanceOf(TimeoutException.class);
        assertThat(blockFuture).isNotDone();
    }

    @Test
    void testTrySync() {
        CompletableFuture<Void> blockFuture = new CompletableFuture<>();
        // this thread will be blocked forever.
        CheckedThread checkedThread = new CheckedThread(blockFuture::get);

        checkedThread.start();

        assertThatNoException()
                .isThrownBy(
                        () -> {
                            checkedThread.trySync(500);
                            assertThat(blockFuture).isNotDone();
                        });
    }
}
