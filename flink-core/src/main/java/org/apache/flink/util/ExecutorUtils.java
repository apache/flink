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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Utilities for {@link java.util.concurrent.Executor Executors}. */
public class ExecutorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);

    /**
     * Gracefully shutdown the given {@link ExecutorService}. The call waits the given timeout that
     * all ExecutorServices terminate. If the ExecutorServices do not terminate in this time, they
     * will be shut down hard.
     *
     * @param timeout to wait for the termination of all ExecutorServices
     * @param unit of the timeout
     * @param executorServices to shut down
     */
    public static void gracefulShutdown(
            long timeout, TimeUnit unit, ExecutorService... executorServices) {
        for (ExecutorService executorService : executorServices) {
            executorService.shutdown();
        }

        boolean wasInterrupted = false;
        final long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
        long timeLeft = unit.toMillis(timeout);
        boolean hasTimeLeft = timeLeft > 0L;

        for (ExecutorService executorService : executorServices) {
            if (wasInterrupted || !hasTimeLeft) {
                executorService.shutdownNow();
            } else {
                try {
                    if (!executorService.awaitTermination(timeLeft, TimeUnit.MILLISECONDS)) {
                        LOG.warn(
                                "ExecutorService did not terminate in time. Shutting it down now.");
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOG.warn(
                            "Interrupted while shutting down executor services. Shutting all "
                                    + "remaining ExecutorServices down now.",
                            e);
                    executorService.shutdownNow();

                    wasInterrupted = true;

                    Thread.currentThread().interrupt();
                }

                timeLeft = endTime - System.currentTimeMillis();
                hasTimeLeft = timeLeft > 0L;
            }
        }
    }

    /**
     * Shuts the given {@link ExecutorService} down in a non-blocking fashion. The shut down will be
     * executed by a thread from the common fork-join pool.
     *
     * <p>The executor services will be shut down gracefully for the given timeout period.
     * Afterwards {@link ExecutorService#shutdownNow()} will be called.
     *
     * @param timeout before {@link ExecutorService#shutdownNow()} is called
     * @param unit time unit of the timeout
     * @param executorServices to shut down
     * @return Future which is completed once the {@link ExecutorService} are shut down
     */
    public static CompletableFuture<Void> nonBlockingShutdown(
            long timeout, TimeUnit unit, ExecutorService... executorServices) {
        return CompletableFuture.supplyAsync(
                () -> {
                    gracefulShutdown(timeout, unit, executorServices);
                    return null;
                });
    }
}
