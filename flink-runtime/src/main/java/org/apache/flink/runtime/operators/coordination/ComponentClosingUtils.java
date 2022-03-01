/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A util class to help with a clean component shutdown. */
public class ComponentClosingUtils {
    private static Clock clock = SystemClock.getInstance();

    /** Utility class, not meant to be instantiated. */
    private ComponentClosingUtils() {}

    /**
     * Close a component with a timeout.
     *
     * @param componentName the name of the component.
     * @param closingSequence the closing logic which is a callable that can throw exceptions.
     * @param closeTimeout the timeout to wait for the component to close.
     * @return An optional throwable which is non-empty if an error occurred when closing the
     *     component.
     */
    public static CompletableFuture<Void> closeAsyncWithTimeout(
            String componentName, Runnable closingSequence, Duration closeTimeout) {
        return closeAsyncWithTimeout(
                componentName, (ThrowingRunnable<Exception>) closingSequence::run, closeTimeout);
    }

    /**
     * Close a component with a timeout.
     *
     * @param componentName the name of the component.
     * @param closingSequence the closing logic.
     * @param closeTimeout the timeout to wait for the component to close.
     * @return An optional throwable which is non-empty if an error occurred when closing the
     *     component.
     */
    public static CompletableFuture<Void> closeAsyncWithTimeout(
            String componentName,
            ThrowingRunnable<Exception> closingSequence,
            Duration closeTimeout) {

        final CompletableFuture<Void> future = new CompletableFuture<>();
        // Start a dedicate thread to close the component.
        final Thread t =
                new Thread(
                        () -> {
                            try {
                                closingSequence.run();
                                future.complete(null);
                            } catch (Throwable error) {
                                future.completeExceptionally(error);
                            }
                        });
        t.start();

        // if the future fails due to a timeout, we interrupt the thread
        future.exceptionally(
                (error) -> {
                    if (error instanceof TimeoutException && t.isAlive()) {
                        abortThread(t);
                    }
                    return null;
                });

        FutureUtils.orTimeout(
                future,
                closeTimeout.toMillis(),
                TimeUnit.MILLISECONDS,
                String.format(
                        "Failed to close the %s before timeout of %d ms",
                        componentName, closeTimeout.toMillis()));

        return future;
    }

    /**
     * A util method that tries to shut down an {@link ExecutorService} elegantly within the given
     * timeout. If the executor has not been shut down before it hits timeout or the thread is
     * interrupted when waiting for the termination, a forceful shutdown will be attempted on the
     * executor.
     *
     * @param executor the {@link ExecutorService} to shut down.
     * @param timeout the timeout duration.
     * @return true if the given executor has been successfully closed, false otherwise.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static boolean tryShutdownExecutorElegantly(ExecutorService executor, Duration timeout) {
        try {
            executor.shutdown();
            executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            // Let it go.
        }
        if (!executor.isTerminated()) {
            shutdownExecutorForcefully(executor, Duration.ZERO, false);
        }
        return executor.isTerminated();
    }

    /**
     * Shutdown the given executor forcefully within the given timeout. The method returns if it is
     * interrupted.
     *
     * @param executor the executor to shut down.
     * @param timeout the timeout duration.
     * @return true if the given executor is terminated, false otherwise.
     */
    public static boolean shutdownExecutorForcefully(ExecutorService executor, Duration timeout) {
        return shutdownExecutorForcefully(executor, timeout, true);
    }

    /**
     * Shutdown the given executor forcefully within the given timeout.
     *
     * @param executor the executor to shut down.
     * @param timeout the timeout duration.
     * @param interruptable when set to true, the method can be interrupted. Each interruption to
     *     the thread results in another {@code ExecutorService.shutdownNow()} call to the shutting
     *     down executor.
     * @return true if the given executor is terminated, false otherwise.
     */
    public static boolean shutdownExecutorForcefully(
            ExecutorService executor, Duration timeout, boolean interruptable) {
        Deadline deadline = Deadline.fromNowWithClock(timeout, clock);
        boolean isInterrupted = false;
        do {
            executor.shutdownNow();
            try {
                executor.awaitTermination(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                isInterrupted = interruptable;
            }
        } while (!isInterrupted && deadline.hasTimeLeft() && !executor.isTerminated());
        return executor.isTerminated();
    }

    private static void abortThread(Thread t) {
        // Try our best here to ensure the thread is aborted. Keep interrupting the
        // thread for 10 times with 10 ms intervals. This helps handle the case
        // where the shutdown sequence consists of a bunch of closeQuietly() calls
        // that will swallow the InterruptedException so the thread to be aborted
        // may block multiple times. If the thread is still alive after all the
        // attempts, just let it go. The caller of closeAsyncWithTimeout() should
        // have received a TimeoutException in this case.
        int i = 0;
        while (t.isAlive() && i < 10) {
            t.interrupt();
            i++;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // Let it go.
            }
        }
    }

    // ========= Method visible for testing ========

    @VisibleForTesting
    static void setClock(Clock clock) {
        ComponentClosingUtils.clock = clock;
    }
}
