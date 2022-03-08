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
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.function.TriFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** Test {@link CompletedCheckpointStore} implementation for testing the shutdown behavior. */
public final class TestingCompletedCheckpointStore implements CompletedCheckpointStore {

    private final TriFunction<
                    CompletedCheckpoint, CheckpointsCleaner, Runnable, CompletedCheckpoint>
            addCheckpointAndSubsumeOldestOneFunction;
    private final BiConsumer<JobStatus, CheckpointsCleaner> shutdownConsumer;
    private final Supplier<List<CompletedCheckpoint>> getAllCheckpointsSupplier;
    private final Supplier<Integer> getNumberOfRetainedCheckpointsSuppler;
    private final Supplier<Integer> getMaxNumberOfRetainedCheckpointsSupplier;
    private final Supplier<Boolean> requiresExternalizedCheckpointsSupplier;
    private final Supplier<SharedStateRegistry> getSharedStateRegistrySupplier;

    public static TestingCompletedCheckpointStore
            createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                    CompletableFuture<JobStatus> shutdownFuture) {
        return TestingCompletedCheckpointStore.builder()
                .withShutdownConsumer(
                        ((jobStatus, ignoredCheckpointsCleaner) ->
                                shutdownFuture.complete(jobStatus)))
                .withGetAllCheckpointsSupplier(Collections::emptyList)
                .build();
    }

    private TestingCompletedCheckpointStore(
            TriFunction<CompletedCheckpoint, CheckpointsCleaner, Runnable, CompletedCheckpoint>
                    addCheckpointAndSubsumeOldestOneFunction,
            BiConsumer<JobStatus, CheckpointsCleaner> shutdownConsumer,
            Supplier<List<CompletedCheckpoint>> getAllCheckpointsSupplier,
            Supplier<Integer> getNumberOfRetainedCheckpointsSuppler,
            Supplier<Integer> getMaxNumberOfRetainedCheckpointsSupplier,
            Supplier<Boolean> requiresExternalizedCheckpointsSupplier,
            Supplier<SharedStateRegistry> getSharedStateRegistrySupplier) {
        this.addCheckpointAndSubsumeOldestOneFunction = addCheckpointAndSubsumeOldestOneFunction;
        this.shutdownConsumer = shutdownConsumer;
        this.getAllCheckpointsSupplier = getAllCheckpointsSupplier;
        this.getNumberOfRetainedCheckpointsSuppler = getNumberOfRetainedCheckpointsSuppler;
        this.getMaxNumberOfRetainedCheckpointsSupplier = getMaxNumberOfRetainedCheckpointsSupplier;
        this.requiresExternalizedCheckpointsSupplier = requiresExternalizedCheckpointsSupplier;
        this.getSharedStateRegistrySupplier = getSharedStateRegistrySupplier;
    }

    @Override
    public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup) {
        return addCheckpointAndSubsumeOldestOneFunction.apply(
                checkpoint, checkpointsCleaner, postCleanup);
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner) {
        shutdownConsumer.accept(jobStatus, checkpointsCleaner);
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() {
        return getAllCheckpointsSupplier.get();
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return getNumberOfRetainedCheckpointsSuppler.get();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return getMaxNumberOfRetainedCheckpointsSupplier.get();
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return requiresExternalizedCheckpointsSupplier.get();
    }

    @Override
    public SharedStateRegistry getSharedStateRegistry() {
        return getSharedStateRegistrySupplier.get();
    }

    public static Builder builder() {
        return new TestingCompletedCheckpointStore.Builder();
    }

    /** {@code Builder} for creating {@code TestingCompletedCheckpointStore} instances. */
    public static class Builder {

        private TriFunction<CompletedCheckpoint, CheckpointsCleaner, Runnable, CompletedCheckpoint>
                addCheckpointAndSubsumeOldestOneFunction =
                        (ignoredCompletedCheckpoint,
                                ignoredCheckpointsCleaner,
                                ignoredPostCleanup) -> {
                            throw new UnsupportedOperationException(
                                    "addCheckpointAndSubsumeOldestOne is not implemented.");
                        };
        private BiConsumer<JobStatus, CheckpointsCleaner> shutdownConsumer =
                (ignoredJobStatus, ignoredCheckpointsCleaner) -> {
                    throw new UnsupportedOperationException("shutdown is not implemented.");
                };
        private Supplier<List<CompletedCheckpoint>> getAllCheckpointsSupplier =
                () -> {
                    throw new UnsupportedOperationException(
                            "getAllCheckpoints is not implemented.");
                };
        private Supplier<Integer> getNumberOfRetainedCheckpointsSuppler =
                () -> {
                    throw new UnsupportedOperationException(
                            "getNumberOfRetainedCheckpointsis not implemented.");
                };
        private Supplier<Integer> getMaxNumberOfRetainedCheckpointsSupplier =
                () -> {
                    throw new UnsupportedOperationException(
                            "getMaxNumberOfRetainedCheckpoints is not implemented.");
                };
        private Supplier<Boolean> requiresExternalizedCheckpointsSupplier =
                () -> {
                    throw new UnsupportedOperationException(
                            "requiresExternalizedCheckpoints is not implemented.");
                };
        private Supplier<SharedStateRegistry> getSharedStateRegistrySupplier =
                () -> {
                    throw new UnsupportedOperationException(
                            "getSharedStateRegistry is not implemented.");
                };

        public Builder withAddCheckpointAndSubsumeOldestOneFunction(
                TriFunction<CompletedCheckpoint, CheckpointsCleaner, Runnable, CompletedCheckpoint>
                        addCheckpointAndSubsumeOldestOneFunction) {
            this.addCheckpointAndSubsumeOldestOneFunction =
                    addCheckpointAndSubsumeOldestOneFunction;
            return this;
        }

        public Builder withShutdownConsumer(
                BiConsumer<JobStatus, CheckpointsCleaner> shutdownConsumer) {
            this.shutdownConsumer = shutdownConsumer;
            return this;
        }

        public Builder withGetAllCheckpointsSupplier(
                Supplier<List<CompletedCheckpoint>> getAllCheckpointsSupplier) {
            this.getAllCheckpointsSupplier = getAllCheckpointsSupplier;
            return this;
        }

        public Builder withGetNumberOfRetainedCheckpointsSupplier(
                Supplier<Integer> getNumberOfRetainedCheckpointsSuppler) {
            this.getNumberOfRetainedCheckpointsSuppler = getNumberOfRetainedCheckpointsSuppler;
            return this;
        }

        public Builder withGetMaxNumberOfRetainedCheckpointsSupplier(
                Supplier<Integer> getMaxNumberOfRetainedCheckpoints) {
            this.getMaxNumberOfRetainedCheckpointsSupplier = getMaxNumberOfRetainedCheckpoints;
            return this;
        }

        public Builder withRequiresExternalizedCheckpointsSupplier(
                Supplier<Boolean> requiresExternalizedCheckpointsSupplier) {
            this.requiresExternalizedCheckpointsSupplier = requiresExternalizedCheckpointsSupplier;
            return this;
        }

        public Builder withGetSharedStateRegistrySupplier(
                Supplier<SharedStateRegistry> getSharedStateRegistrySupplier) {
            this.getSharedStateRegistrySupplier = getSharedStateRegistrySupplier;
            return this;
        }

        public TestingCompletedCheckpointStore build() {
            return new TestingCompletedCheckpointStore(
                    addCheckpointAndSubsumeOldestOneFunction,
                    shutdownConsumer,
                    getAllCheckpointsSupplier,
                    getNumberOfRetainedCheckpointsSuppler,
                    getMaxNumberOfRetainedCheckpointsSupplier,
                    requiresExternalizedCheckpointsSupplier,
                    getSharedStateRegistrySupplier);
        }
    }
}
