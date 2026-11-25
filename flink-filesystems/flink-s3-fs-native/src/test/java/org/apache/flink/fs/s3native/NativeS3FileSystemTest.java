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

package org.apache.flink.fs.s3native;

import org.apache.flink.util.TemporaryClassLoaderContext;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeS3FileSystem}. */
class NativeS3FileSystemTest {

    @Test
    void uploadThreadsDoNotKeepTheContextClassLoaderOfTheWritingThread() throws Exception {
        final ExecutorService uploadThreadPool = NativeS3FileSystem.createUploadThreadPool(1);
        // Stands in for the user code class loader of the task thread that writes to a stream.
        try (URLClassLoader userCodeClassLoader = new URLClassLoader(new URL[0], null);
                TemporaryClassLoaderContext ignored =
                        TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            // The pool has no thread yet, so this thread starts the one that runs the task.
            final ClassLoader uploadThreadClassLoader =
                    uploadThreadPool
                            .submit(() -> Thread.currentThread().getContextClassLoader())
                            .get();

            assertThat(uploadThreadClassLoader).isSameAs(NativeS3FileSystem.class.getClassLoader());
        } finally {
            uploadThreadPool.shutdownNow();
        }
    }

    @Test
    void uploadThreadPoolQueuesUploadsBeyondItsBound() throws Exception {
        final ThreadPoolExecutor uploadThreadPool = NativeS3FileSystem.createUploadThreadPool(2);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            for (int i = 0; i < 5; i++) {
                uploadThreadPool.execute(
                        () -> {
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }

            assertThat(uploadThreadPool.getPoolSize()).isEqualTo(2);
            assertThat(uploadThreadPool.getQueue()).hasSize(3);
        } finally {
            release.countDown();
            uploadThreadPool.shutdownNow();
        }
    }
}
