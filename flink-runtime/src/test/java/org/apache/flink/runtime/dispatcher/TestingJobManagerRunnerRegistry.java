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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@code TestingJobManagerRunnerRegistry} is a test implementation of {@link
 * JobManagerRunnerRegistry}.
 */
public class TestingJobManagerRunnerRegistry implements JobManagerRunnerRegistry {

    private final Function<JobID, Boolean> isRegisteredFunction;
    private final Consumer<JobManagerRunner> registerConsumer;
    private final Function<JobID, JobManagerRunner> getFunction;
    private final Supplier<Integer> sizeSupplier;
    private final Supplier<Set<JobID>> getRunningJobIdsSupplier;
    private final Supplier<Collection<JobManagerRunner>> getJobManagerRunnersSupplier;
    private final Function<JobID, JobManagerRunner> unregisterFunction;
    private final BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupAsyncFunction;

    private TestingJobManagerRunnerRegistry(
            Function<JobID, Boolean> isRegisteredFunction,
            Consumer<JobManagerRunner> registerConsumer,
            Function<JobID, JobManagerRunner> getFunction,
            Supplier<Integer> sizeSupplier,
            Supplier<Set<JobID>> getRunningJobIdsSupplier,
            Supplier<Collection<JobManagerRunner>> getJobManagerRunnersSupplier,
            Function<JobID, JobManagerRunner> unregisterFunction,
            BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupAsyncFunction) {
        this.isRegisteredFunction = isRegisteredFunction;
        this.registerConsumer = registerConsumer;
        this.getFunction = getFunction;
        this.sizeSupplier = sizeSupplier;
        this.getRunningJobIdsSupplier = getRunningJobIdsSupplier;
        this.getJobManagerRunnersSupplier = getJobManagerRunnersSupplier;
        this.unregisterFunction = unregisterFunction;
        this.localCleanupAsyncFunction = localCleanupAsyncFunction;
    }

    @Override
    public boolean isRegistered(JobID jobId) {
        return isRegisteredFunction.apply(jobId);
    }

    @Override
    public void register(JobManagerRunner jobManagerRunner) {
        registerConsumer.accept(jobManagerRunner);
    }

    @Override
    public JobManagerRunner get(JobID jobId) {
        return getFunction.apply(jobId);
    }

    @Override
    public int size() {
        return sizeSupplier.get();
    }

    @Override
    public Set<JobID> getRunningJobIds() {
        return getRunningJobIdsSupplier.get();
    }

    @Override
    public Collection<JobManagerRunner> getJobManagerRunners() {
        return getJobManagerRunnersSupplier.get();
    }

    @Override
    public JobManagerRunner unregister(JobID jobId) {
        return unregisterFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        return localCleanupAsyncFunction.apply(jobId, executor);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a {@code Builder} that simulates the {@link JobManagerRunnerRegistry} with at most
     * one underlying {@link JobManagerRunner}. The default test implementation follows the {@code
     * JobManagerRunnerRegistry} contract. The underlying {@code JobManagerRunner} instance is held
     * by the {@code AtomicReference}, which makes it accessible to the test code.
     */
    public static Builder newSingleJobBuilder(
            AtomicReference<JobManagerRunner> singleRunnerReference) {
        return builder()
                .withRegisterConsumer(
                        jobManagerRunner -> {
                            Preconditions.checkState(singleRunnerReference.get() == null);
                            singleRunnerReference.set(jobManagerRunner);
                        })
                .withIsRegisteredFunction(
                        actualJobId ->
                                Optional.ofNullable(singleRunnerReference.get())
                                        .map(JobManagerRunner::getJobID)
                                        .map(actualJobId::equals)
                                        .orElse(false))
                .withGetFunction(
                        actualJobId ->
                                Optional.ofNullable(singleRunnerReference.get())
                                        .orElseThrow(throwNoSuchElementException(actualJobId)))
                .withGetJobManagerRunnersSupplier(
                        () -> CollectionUtil.ofNullable(singleRunnerReference.get()))
                .withSizeSupplier(
                        () ->
                                Optional.ofNullable(singleRunnerReference.get())
                                        .map(ignored -> 1)
                                        .orElse(0))
                .withGetRunningJobIdsSupplier(
                        () ->
                                Optional.ofNullable(singleRunnerReference.get())
                                        .map(JobManagerRunner::getJobID)
                                        .map(Collections::singleton)
                                        .orElse(Collections.emptySet()))
                .withUnregisterFunction(
                        actualJobId ->
                                unregisterFromReference(singleRunnerReference, actualJobId)
                                        .orElseThrow(throwNoSuchElementException(actualJobId)))
                .withLocalCleanupAsyncFunction(
                        (actualJobId, executor) ->
                                unregisterFromReference(singleRunnerReference, actualJobId)
                                        .map(JobManagerRunner::closeAsync)
                                        .orElse(FutureUtils.completedVoidFuture()));
    }

    private static Optional<JobManagerRunner> unregisterFromReference(
            AtomicReference<JobManagerRunner> singleRunnerReference, JobID actualJobId) {
        return Optional.ofNullable(singleRunnerReference.get())
                .map(JobManagerRunner::getJobID)
                .filter(actualJobId::equals)
                .map(ignored -> singleRunnerReference.getAndSet(null));
    }

    private static Supplier<NoSuchElementException> throwNoSuchElementException(JobID jobId) {
        return () ->
                new NoSuchElementException(
                        "JobManagerRunner with job ID " + jobId + " is not registered.");
    }

    /** {@code Builder} for creating {@code TestingJobManagerRunnerRegistry} instances. */
    public static class Builder {

        private Function<JobID, Boolean> isRegisteredFunction = ignoredJobId -> true;
        private Consumer<JobManagerRunner> registerConsumer = ignoredRunner -> {};
        private Function<JobID, JobManagerRunner> getFunction = ignoredJobId -> null;
        private Supplier<Integer> sizeSupplier = () -> 0;
        private Supplier<Set<JobID>> getRunningJobIdsSupplier = Collections::emptySet;
        private Supplier<Collection<JobManagerRunner>> getJobManagerRunnersSupplier =
                Collections::emptyList;
        private Function<JobID, JobManagerRunner> unregisterFunction = ignoredJobId -> null;
        private BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupAsyncFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();
        private BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupAsyncFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        public Builder withIsRegisteredFunction(Function<JobID, Boolean> isRegisteredFunction) {
            this.isRegisteredFunction = isRegisteredFunction;
            return this;
        }

        public Builder withRegisterConsumer(Consumer<JobManagerRunner> registerConsumer) {
            this.registerConsumer = registerConsumer;
            return this;
        }

        public Builder withGetFunction(Function<JobID, JobManagerRunner> getFunction) {
            this.getFunction = getFunction;
            return this;
        }

        public Builder withSizeSupplier(Supplier<Integer> sizeSupplier) {
            this.sizeSupplier = sizeSupplier;
            return this;
        }

        public Builder withGetRunningJobIdsSupplier(Supplier<Set<JobID>> getRunningJobIdsSupplier) {
            this.getRunningJobIdsSupplier = getRunningJobIdsSupplier;
            return this;
        }

        public Builder withGetJobManagerRunnersSupplier(
                Supplier<Collection<JobManagerRunner>> getJobManagerRunnersSupplier) {
            this.getJobManagerRunnersSupplier = getJobManagerRunnersSupplier;
            return this;
        }

        public Builder withUnregisterFunction(
                Function<JobID, JobManagerRunner> unregisterFunction) {
            this.unregisterFunction = unregisterFunction;
            return this;
        }

        public Builder withLocalCleanupAsyncFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupAsyncFunction) {
            this.localCleanupAsyncFunction = localCleanupAsyncFunction;
            return this;
        }

        public TestingJobManagerRunnerRegistry build() {
            return new TestingJobManagerRunnerRegistry(
                    isRegisteredFunction,
                    registerConsumer,
                    getFunction,
                    sizeSupplier,
                    getRunningJobIdsSupplier,
                    getJobManagerRunnersSupplier,
                    unregisterFunction,
                    localCleanupAsyncFunction);
        }
    }
}
