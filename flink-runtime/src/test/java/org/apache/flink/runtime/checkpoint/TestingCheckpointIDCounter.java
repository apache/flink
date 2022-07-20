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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Test {@link CheckpointIDCounter} implementation for testing the shutdown behavior. */
public final class TestingCheckpointIDCounter implements CheckpointIDCounter {

    private final Runnable startRunnable;
    private final Function<JobStatus, CompletableFuture<Void>> shutdownFunction;
    private final Supplier<Integer> getAndIncrementSupplier;
    private final Supplier<Integer> getSupplier;
    private final Consumer<Long> setCountConsumer;

    public static TestingCheckpointIDCounter createStoreWithShutdownCheckAndNoStartAction(
            CompletableFuture<JobStatus> shutdownFuture) {
        return TestingCheckpointIDCounter.builder()
                .withStartRunnable(() -> {})
                .withShutdownConsumer(
                        jobStatus -> {
                            shutdownFuture.complete(jobStatus);
                            return FutureUtils.completedVoidFuture();
                        })
                .build();
    }

    private TestingCheckpointIDCounter(
            Runnable startRunnable,
            Function<JobStatus, CompletableFuture<Void>> shutdownFunction,
            Supplier<Integer> getAndIncrementSupplier,
            Supplier<Integer> getSupplier,
            Consumer<Long> setCountConsumer) {
        this.startRunnable = startRunnable;
        this.shutdownFunction = shutdownFunction;
        this.getAndIncrementSupplier = getAndIncrementSupplier;
        this.getSupplier = getSupplier;
        this.setCountConsumer = setCountConsumer;
    }

    @Override
    public void start() {
        startRunnable.run();
    }

    @Override
    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
        return shutdownFunction.apply(jobStatus);
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementSupplier.get();
    }

    @Override
    public long get() {
        return getSupplier.get();
    }

    @Override
    public void setCount(long newId) {
        setCountConsumer.accept(newId);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingCheckpointIDCounter} instances. */
    public static class Builder {

        private Runnable startRunnable;
        private Function<JobStatus, CompletableFuture<Void>> shutdownFunction;
        private Supplier<Integer> getAndIncrementSupplier;
        private Supplier<Integer> getSupplier;
        private Consumer<Long> setCountConsumer;

        public Builder withStartRunnable(Runnable startRunnable) {
            this.startRunnable = startRunnable;
            return this;
        }

        public Builder withShutdownConsumer(
                Function<JobStatus, CompletableFuture<Void>> shutdownFunction) {
            this.shutdownFunction = shutdownFunction;
            return this;
        }

        public Builder withGetAndIncrementSupplier(Supplier<Integer> getAndIncrementSupplier) {
            this.getAndIncrementSupplier = getAndIncrementSupplier;
            return this;
        }

        public Builder withGetSupplier(Supplier<Integer> getSupplier) {
            this.getSupplier = getSupplier;
            return this;
        }

        public Builder withSetCountConsumer(Consumer<Long> setCountConsumer) {
            this.setCountConsumer = setCountConsumer;
            return this;
        }

        public TestingCheckpointIDCounter build() {
            return new TestingCheckpointIDCounter(
                    startRunnable,
                    shutdownFunction,
                    getAndIncrementSupplier,
                    getSupplier,
                    setCountConsumer);
        }
    }
}
