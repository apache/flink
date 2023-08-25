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

package org.apache.flink.runtime.checkpoint.hooks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the MasterHooks utility class. */
class MasterHooksTest {

    // ------------------------------------------------------------------------
    //  hook management
    // ------------------------------------------------------------------------

    @Test
    void wrapHook() throws Exception {
        final String id = "id";

        Thread thread = Thread.currentThread();
        final ClassLoader originalClassLoader = thread.getContextClassLoader();
        final ClassLoader userClassLoader = new URLClassLoader(new URL[0]);

        final CompletableFuture<Void> onceRunnableFuture = new CompletableFuture<>();
        final Runnable onceRunnable =
                () -> {
                    assertThat(Thread.currentThread().getContextClassLoader())
                            .isEqualTo(userClassLoader);
                    assertThat(onceRunnableFuture)
                            .withFailMessage("The runnable shouldn't be called multiple times.")
                            .isNotDone();
                    onceRunnableFuture.complete(null);
                };

        final CompletableFuture<Void> getIdentifierFuture = new CompletableFuture<>();
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final CompletableFuture<Void> restoreCheckpointFuture = new CompletableFuture<>();
        final CompletableFuture<Void> createCheckpointDataSerializerFuture =
                new CompletableFuture<>();
        MasterTriggerRestoreHook<String> hook =
                new MasterTriggerRestoreHook<String>() {
                    @Override
                    public String getIdentifier() {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);

                        assertThat(getIdentifierFuture)
                                .withFailMessage("The method shouldn't be called multiple times.")
                                .isNotDone();
                        getIdentifierFuture.complete(null);
                        return id;
                    }

                    @Override
                    public void reset() {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);
                    }

                    @Override
                    public void close() {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);
                        assertThat(closeFuture)
                                .withFailMessage("The method shouldn't be called multiple times.")
                                .isNotDone();
                        closeFuture.complete(null);
                    }

                    @Nullable
                    @Override
                    public CompletableFuture<String> triggerCheckpoint(
                            long checkpointId, long timestamp, Executor executor) {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);
                        executor.execute(onceRunnable);
                        return null;
                    }

                    @Override
                    public void restoreCheckpoint(
                            long checkpointId, @Nullable String checkpointData) {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);

                        assertThat(checkpointId).isZero();
                        assertThat(checkpointData).isEmpty();
                        assertThat(restoreCheckpointFuture)
                                .withFailMessage("The method shouldn't be called multiple times.")
                                .isNotDone();
                        restoreCheckpointFuture.complete(null);
                    }

                    @Nullable
                    @Override
                    public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isEqualTo(userClassLoader);

                        assertThat(createCheckpointDataSerializerFuture)
                                .withFailMessage("The method shouldn't be called multiple times.")
                                .isNotDone();
                        createCheckpointDataSerializerFuture.complete(null);
                        return null;
                    }
                };

        MasterTriggerRestoreHook<String> wrapped = MasterHooks.wrapHook(hook, userClassLoader);

        // verify getIdentifier
        wrapped.getIdentifier();

        assertThat(getIdentifierFuture).isCompleted();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify triggerCheckpoint and its wrapped executor
        TestExecutor testExecutor = new TestExecutor();
        wrapped.triggerCheckpoint(0L, 0, testExecutor);
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);
        assertThat(testExecutor.command).isNotNull();
        testExecutor.command.run();
        assertThat(onceRunnableFuture).isCompleted();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify restoreCheckpoint
        wrapped.restoreCheckpoint(0L, "");
        assertThat(restoreCheckpointFuture).isCompleted();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify createCheckpointDataSerializer
        wrapped.createCheckpointDataSerializer();
        assertThat(createCheckpointDataSerializerFuture).isCompleted();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify close
        wrapped.close();
        assertThat(closeFuture).isCompleted();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);
    }

    private static class TestExecutor implements Executor {
        Runnable command;

        @Override
        public void execute(@NotNull Runnable command) {
            this.command = command;
        }
    }
}
