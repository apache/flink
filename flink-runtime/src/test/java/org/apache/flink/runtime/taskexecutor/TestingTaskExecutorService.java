/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import java.util.concurrent.CompletableFuture;

/** Testing implementation of {@link TaskManagerRunner.TaskExecutorService}. */
public class TestingTaskExecutorService implements TaskManagerRunner.TaskExecutorService {
    private final Runnable startRunnable;
    private final CompletableFuture<Void> terminationFuture;
    private final boolean completeTerminationFutureOnClose;

    private TestingTaskExecutorService(
            Runnable startRunnable,
            CompletableFuture<Void> terminationFuture,
            boolean completeTerminationFutureOnClose) {
        this.startRunnable = startRunnable;
        this.terminationFuture = terminationFuture;
        this.completeTerminationFutureOnClose = completeTerminationFutureOnClose;
    }

    @Override
    public void start() {
        startRunnable.run();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (completeTerminationFutureOnClose) {
            terminationFuture.complete(null);
        }
        return terminationFuture;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingTaskExecutorService}. */
    public static final class Builder {
        private Runnable startRunnable = () -> {};
        private CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        private boolean completeTerminationFutureOnClose = true;

        public Builder setStartRunnable(Runnable startRunnable) {
            this.startRunnable = startRunnable;
            return this;
        }

        public Builder setTerminationFuture(CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
            return this;
        }

        public Builder withManualTerminationFutureCompletion() {
            completeTerminationFutureOnClose = false;
            return this;
        }

        TestingTaskExecutorService build() {
            return new TestingTaskExecutorService(
                    startRunnable, terminationFuture, completeTerminationFutureOnClose);
        }
    }
}
