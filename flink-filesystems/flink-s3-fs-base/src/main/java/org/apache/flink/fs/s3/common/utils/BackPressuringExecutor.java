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

package org.apache.flink.fs.s3.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An executor decorator that allows only a certain number of concurrent executions. The {@link
 * #execute(Runnable)} method blocks once that number of executions is exceeded.
 */
@Internal
public final class BackPressuringExecutor implements Executor {

    /** The executor for the actual execution. */
    private final Executor delegate;

    /** The semaphore to track permits and block until permits are available. */
    private final Semaphore permits;

    public BackPressuringExecutor(Executor delegate, int numConcurrentExecutions) {
        checkArgument(numConcurrentExecutions > 0, "numConcurrentExecutions must be > 0");
        this.delegate = checkNotNull(delegate, "delegate");
        this.permits = new Semaphore(numConcurrentExecutions, true);
    }

    @Override
    public void execute(Runnable command) {
        // To not block interrupts here (faster cancellation) we acquire interruptibly.
        // Unfortunately, we need to rethrow this as a RuntimeException (suboptimal), because
        // the method signature does not permit anything else, and we want to maintain the
        // Executor interface for transparent drop-in.
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException("interrupted:", e);
        }

        final SemaphoreReleasingRunnable runnable =
                new SemaphoreReleasingRunnable(command, permits);
        try {
            delegate.execute(runnable);
        } catch (Throwable e) {
            runnable.release();
            ExceptionUtils.rethrow(e, e.getMessage());
        }
    }

    // ------------------------------------------------------------------------

    private static class SemaphoreReleasingRunnable implements Runnable {

        private final Runnable delegate;

        private final Semaphore toRelease;

        private final AtomicBoolean released = new AtomicBoolean();

        SemaphoreReleasingRunnable(Runnable delegate, Semaphore toRelease) {
            this.delegate = delegate;
            this.toRelease = toRelease;
        }

        @Override
        public void run() {
            try {
                delegate.run();
            } finally {
                release();
            }
        }

        void release() {
            if (released.compareAndSet(false, true)) {
                toRelease.release();
            }
        }
    }
}
