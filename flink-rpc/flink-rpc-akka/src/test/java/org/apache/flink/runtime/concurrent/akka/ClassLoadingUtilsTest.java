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

package org.apache.flink.runtime.concurrent.akka;

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/** Tests for the {@link ClassLoadingUtils}. */
public class ClassLoadingUtilsTest extends TestLogger {

    private static final ClassLoader TEST_CLASS_LOADER =
            new URLClassLoader(new URL[0], ClassLoadingUtilsTest.class.getClassLoader());

    @Test
    public void testRunnableWithContextClassLoader() throws Exception {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        Runnable runnable =
                () -> contextClassLoader.complete(Thread.currentThread().getContextClassLoader());

        final Runnable wrappedRunnable =
                ClassLoadingUtils.withContextClassLoader(runnable, TEST_CLASS_LOADER);

        // the runnable should only be wrapped, not run immediately
        assertThat(contextClassLoader.isDone(), is(false));

        wrappedRunnable.run();
        assertThat(contextClassLoader.get(), is(TEST_CLASS_LOADER));
    }

    @Test
    public void testExecutorWithContextClassLoader() throws Exception {
        final Executor wrappedExecutor =
                ClassLoadingUtils.withContextClassLoader(
                        Executors.newDirectExecutorService(), TEST_CLASS_LOADER);

        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        Runnable runnable =
                () -> contextClassLoader.complete(Thread.currentThread().getContextClassLoader());

        wrappedExecutor.execute(runnable);
        assertThat(contextClassLoader.get(), is(TEST_CLASS_LOADER));
    }

    @Test
    public void testRunRunnableWithContextClassLoader() throws Exception {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        ThrowingRunnable<Exception> runnable =
                () -> contextClassLoader.complete(Thread.currentThread().getContextClassLoader());

        ClassLoadingUtils.runWithContextClassLoader(runnable, TEST_CLASS_LOADER);
        assertThat(contextClassLoader.get(), is(TEST_CLASS_LOADER));
    }

    @Test
    public void testRunSupplierWithContextClassLoader() throws Exception {
        SupplierWithException<ClassLoader, Exception> runnable =
                () -> Thread.currentThread().getContextClassLoader();

        final ClassLoader contextClassLoader =
                ClassLoadingUtils.runWithContextClassLoader(runnable, TEST_CLASS_LOADER);
        assertThat(contextClassLoader, is(TEST_CLASS_LOADER));
    }

    @Test
    public void testGuardCompletionWithContextClassLoader() throws Exception {
        final CompletableFuture<Void> originalFuture = new CompletableFuture<>();

        final CompletableFuture<Void> guardedFuture =
                ClassLoadingUtils.guardCompletionWithContextClassLoader(
                        originalFuture, TEST_CLASS_LOADER);

        final CompletableFuture<ClassLoader> contextClassLoader =
                guardedFuture.thenApply(ignored -> Thread.currentThread().getContextClassLoader());

        originalFuture.complete(null);
        assertThat(contextClassLoader.get(), is(TEST_CLASS_LOADER));
    }
}
