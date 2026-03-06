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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.jobmanager.ApplicationStore;
import org.apache.flink.runtime.jobmanager.ApplicationStoreEntry;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/** In-Memory implementation of {@link ApplicationStore} for testing purposes. */
public class TestingApplicationStore implements ApplicationStore {

    private final Map<ApplicationID, ApplicationStoreEntry> storedApplications = new HashMap<>();

    private final ThrowingConsumer<Void, ? extends Exception> startConsumer;

    private final ThrowingRunnable<? extends Exception> stopRunnable;

    private final FunctionWithException<
                    Collection<ApplicationID>, Collection<ApplicationID>, ? extends Exception>
            applicationIdsFunction;

    private final BiFunctionWithException<
                    ApplicationID,
                    Map<ApplicationID, ApplicationStoreEntry>,
                    ApplicationStoreEntry,
                    ? extends Exception>
            recoverApplicationFunction;

    private final ThrowingConsumer<ApplicationStoreEntry, ? extends Exception>
            putApplicationConsumer;

    private final BiFunction<ApplicationID, Executor, CompletableFuture<Void>>
            globalCleanupFunction;

    private boolean started;

    private TestingApplicationStore(
            ThrowingConsumer<Void, ? extends Exception> startConsumer,
            ThrowingRunnable<? extends Exception> stopRunnable,
            FunctionWithException<
                            Collection<ApplicationID>,
                            Collection<ApplicationID>,
                            ? extends Exception>
                    applicationIdsFunction,
            BiFunctionWithException<
                            ApplicationID,
                            Map<ApplicationID, ApplicationStoreEntry>,
                            ApplicationStoreEntry,
                            ? extends Exception>
                    recoverApplicationFunction,
            ThrowingConsumer<ApplicationStoreEntry, ? extends Exception> putApplicationConsumer,
            BiFunction<ApplicationID, Executor, CompletableFuture<Void>> globalCleanupFunction,
            Collection<ApplicationStoreEntry> initialApplications) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.applicationIdsFunction = applicationIdsFunction;
        this.recoverApplicationFunction = recoverApplicationFunction;
        this.putApplicationConsumer = putApplicationConsumer;
        this.globalCleanupFunction = globalCleanupFunction;

        for (ApplicationStoreEntry initialApplication : initialApplications) {
            storedApplications.put(initialApplication.getApplicationId(), initialApplication);
        }
    }

    @Override
    public synchronized void start() throws Exception {
        startConsumer.accept(null);
        started = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        stopRunnable.run();
        started = false;
    }

    @Override
    public synchronized Optional<ApplicationStoreEntry> recoverApplication(
            ApplicationID applicationId) throws Exception {
        verifyIsStarted();
        return Optional.ofNullable(
                recoverApplicationFunction.apply(applicationId, storedApplications));
    }

    @Override
    public synchronized void putApplication(ApplicationStoreEntry applicationStoreEntry)
            throws Exception {
        verifyIsStarted();
        putApplicationConsumer.accept(applicationStoreEntry);
        storedApplications.put(applicationStoreEntry.getApplicationId(), applicationStoreEntry);
    }

    @Override
    public synchronized CompletableFuture<Void> globalCleanupAsync(
            ApplicationID applicationId, Executor executor) {
        verifyIsStarted();
        return globalCleanupFunction
                .apply(applicationId, executor)
                .thenRun(() -> storedApplications.remove(applicationId));
    }

    @Override
    public synchronized Collection<ApplicationID> getApplicationIds() throws Exception {
        verifyIsStarted();
        return applicationIdsFunction.apply(
                Collections.unmodifiableSet(new HashSet<>(storedApplications.keySet())));
    }

    public synchronized boolean contains(ApplicationID applicationId) {
        return storedApplications.containsKey(applicationId);
    }

    private void verifyIsStarted() {
        Preconditions.checkState(started, "Not running. Forgot to call start()?");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingApplicationStore} instances. */
    public static class Builder {
        private ThrowingConsumer<Void, ? extends Exception> startConsumer = ignored -> {};

        private ThrowingRunnable<? extends Exception> stopRunnable = () -> {};

        private FunctionWithException<
                        Collection<ApplicationID>, Collection<ApplicationID>, ? extends Exception>
                applicationIdsFunction = applicationIds -> applicationIds;

        private BiFunctionWithException<
                        ApplicationID,
                        Map<ApplicationID, ApplicationStoreEntry>,
                        ApplicationStoreEntry,
                        ? extends Exception>
                recoverApplicationFunction =
                        (applicationId, applications) -> applications.get(applicationId);

        private ThrowingConsumer<ApplicationStoreEntry, ? extends Exception>
                putApplicationConsumer = ignored -> {};

        private BiFunction<ApplicationID, Executor, CompletableFuture<Void>> globalCleanupFunction =
                (ignoredApplicationId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private Collection<ApplicationStoreEntry> initialApplications = Collections.emptyList();

        private boolean startApplicationStore = false;

        private Builder() {}

        public Builder setStartConsumer(ThrowingConsumer<Void, ? extends Exception> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(ThrowingRunnable<? extends Exception> stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setApplicationIdsFunction(
                FunctionWithException<
                                Collection<ApplicationID>,
                                Collection<ApplicationID>,
                                ? extends Exception>
                        applicationIdsFunction) {
            this.applicationIdsFunction = applicationIdsFunction;
            return this;
        }

        public Builder setRecoverApplicationFunction(
                BiFunctionWithException<
                                ApplicationID,
                                Map<ApplicationID, ApplicationStoreEntry>,
                                ApplicationStoreEntry,
                                ? extends Exception>
                        recoverApplicationFunction) {
            this.recoverApplicationFunction = recoverApplicationFunction;
            return this;
        }

        public Builder setPutApplicationConsumer(
                ThrowingConsumer<ApplicationStoreEntry, ? extends Exception>
                        putApplicationConsumer) {
            this.putApplicationConsumer = putApplicationConsumer;
            return this;
        }

        public Builder setGlobalCleanupFunction(
                BiFunction<ApplicationID, Executor, CompletableFuture<Void>>
                        globalCleanupFunction) {
            this.globalCleanupFunction = globalCleanupFunction;
            return this;
        }

        public Builder setInitialApplications(
                Collection<ApplicationStoreEntry> initialApplications) {
            this.initialApplications = initialApplications;
            return this;
        }

        public Builder withAutomaticStart() {
            this.startApplicationStore = true;
            return this;
        }

        public TestingApplicationStore build() {
            final TestingApplicationStore applicationStore =
                    new TestingApplicationStore(
                            startConsumer,
                            stopRunnable,
                            applicationIdsFunction,
                            recoverApplicationFunction,
                            putApplicationConsumer,
                            globalCleanupFunction,
                            initialApplications);

            if (startApplicationStore) {
                try {
                    applicationStore.start();
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            }

            return applicationStore;
        }
    }
}
