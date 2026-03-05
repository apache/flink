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
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.highavailability.ApplicationResultStore;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@code TestingApplicationResultStore} is an {@link ApplicationResultStore} implementation that
 * can be used in tests.
 */
public class TestingApplicationResultStore implements ApplicationResultStore {

    public static final ApplicationResult DUMMY_APPLICATION_RESULT =
            createSuccessfulApplicationResult(new ApplicationID());

    public static ApplicationResult createSuccessfulApplicationResult(ApplicationID applicationId) {
        return createApplicationResult(applicationId, ApplicationState.FINISHED);
    }

    public static ApplicationResult createApplicationResult(
            ApplicationID applicationId, ApplicationState applicationState) {
        return new ApplicationResult(applicationId, applicationState);
    }

    private final Function<ApplicationResultEntry, CompletableFuture<Void>>
            createDirtyResultConsumer;
    private final Function<ApplicationID, CompletableFuture<Void>> markResultAsCleanConsumer;

    private final Function<ApplicationID, CompletableFuture<Boolean>>
            hasApplicationResultEntryFunction;
    private final Function<ApplicationID, CompletableFuture<Boolean>>
            hasDirtyApplicationResultEntryFunction;
    private final Function<ApplicationID, CompletableFuture<Boolean>>
            hasCleanApplicationResultEntryFunction;
    private final SupplierWithException<Set<ApplicationResult>, ? extends IOException>
            getDirtyResultsSupplier;

    private TestingApplicationResultStore(
            Function<ApplicationResultEntry, CompletableFuture<Void>> createDirtyResultConsumer,
            Function<ApplicationID, CompletableFuture<Void>> markResultAsCleanConsumer,
            Function<ApplicationID, CompletableFuture<Boolean>> hasApplicationResultEntryFunction,
            Function<ApplicationID, CompletableFuture<Boolean>>
                    hasDirtyApplicationResultEntryFunction,
            Function<ApplicationID, CompletableFuture<Boolean>>
                    hasCleanApplicationResultEntryFunction,
            SupplierWithException<Set<ApplicationResult>, ? extends IOException>
                    getDirtyResultsSupplier) {
        this.createDirtyResultConsumer = createDirtyResultConsumer;
        this.markResultAsCleanConsumer = markResultAsCleanConsumer;
        this.hasApplicationResultEntryFunction = hasApplicationResultEntryFunction;
        this.hasDirtyApplicationResultEntryFunction = hasDirtyApplicationResultEntryFunction;
        this.hasCleanApplicationResultEntryFunction = hasCleanApplicationResultEntryFunction;
        this.getDirtyResultsSupplier = getDirtyResultsSupplier;
    }

    @Override
    public CompletableFuture<Void> createDirtyResultAsync(
            ApplicationResultEntry applicationResultEntry) {
        return createDirtyResultConsumer.apply(applicationResultEntry);
    }

    @Override
    public CompletableFuture<Void> markResultAsCleanAsync(ApplicationID applicationId) {
        return markResultAsCleanConsumer.apply(applicationId);
    }

    @Override
    public CompletableFuture<Boolean> hasApplicationResultEntryAsync(ApplicationID applicationId) {
        return hasApplicationResultEntryFunction.apply(applicationId);
    }

    @Override
    public CompletableFuture<Boolean> hasDirtyApplicationResultEntryAsync(
            ApplicationID applicationId) {
        return hasDirtyApplicationResultEntryFunction.apply(applicationId);
    }

    @Override
    public CompletableFuture<Boolean> hasCleanApplicationResultEntryAsync(
            ApplicationID applicationId) {
        return hasCleanApplicationResultEntryFunction.apply(applicationId);
    }

    @Override
    public Set<ApplicationResult> getDirtyResults() throws IOException {
        return getDirtyResultsSupplier.get();
    }

    public static TestingApplicationResultStore.Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for instantiating {@code TestingApplicationResultStore} instances. */
    public static class Builder {

        private Function<ApplicationResultEntry, CompletableFuture<Void>>
                createDirtyResultConsumer =
                        applicationResultEntry -> FutureUtils.completedVoidFuture();
        private Function<ApplicationID, CompletableFuture<Void>> markResultAsCleanConsumer =
                applicationID -> FutureUtils.completedVoidFuture();

        private Function<ApplicationID, CompletableFuture<Boolean>>
                hasApplicationResultEntryFunction =
                        applicationID -> CompletableFuture.completedFuture(false);
        private Function<ApplicationID, CompletableFuture<Boolean>>
                hasDirtyApplicationResultEntryFunction =
                        applicationID -> CompletableFuture.completedFuture(false);
        private Function<ApplicationID, CompletableFuture<Boolean>>
                hasCleanApplicationResultEntryFunction =
                        applicationID -> CompletableFuture.completedFuture(false);

        private SupplierWithException<Set<ApplicationResult>, ? extends IOException>
                getDirtyResultsSupplier = Collections::emptySet;

        public Builder withCreateDirtyResultConsumer(
                Function<ApplicationResultEntry, CompletableFuture<Void>>
                        createDirtyResultConsumer) {
            this.createDirtyResultConsumer = createDirtyResultConsumer;
            return this;
        }

        public Builder withMarkResultAsCleanConsumer(
                Function<ApplicationID, CompletableFuture<Void>> markResultAsCleanConsumer) {
            this.markResultAsCleanConsumer = markResultAsCleanConsumer;
            return this;
        }

        public Builder withHasApplicationResultEntryFunction(
                Function<ApplicationID, CompletableFuture<Boolean>>
                        hasApplicationResultEntryFunction) {
            this.hasApplicationResultEntryFunction = hasApplicationResultEntryFunction;
            return this;
        }

        public Builder withHasDirtyApplicationResultEntryFunction(
                Function<ApplicationID, CompletableFuture<Boolean>>
                        hasDirtyApplicationResultEntryFunction) {
            this.hasDirtyApplicationResultEntryFunction = hasDirtyApplicationResultEntryFunction;
            return this;
        }

        public Builder withHasCleanApplicationResultEntryFunction(
                Function<ApplicationID, CompletableFuture<Boolean>>
                        hasCleanApplicationResultEntryFunction) {
            this.hasCleanApplicationResultEntryFunction = hasCleanApplicationResultEntryFunction;
            return this;
        }

        public Builder withGetDirtyResultsSupplier(
                SupplierWithException<Set<ApplicationResult>, ? extends IOException>
                        getDirtyResultsSupplier) {
            this.getDirtyResultsSupplier = getDirtyResultsSupplier;
            return this;
        }

        public TestingApplicationResultStore build() {
            return new TestingApplicationResultStore(
                    createDirtyResultConsumer,
                    markResultAsCleanConsumer,
                    hasApplicationResultEntryFunction,
                    hasDirtyApplicationResultEntryFunction,
                    hasCleanApplicationResultEntryFunction,
                    getDirtyResultsSupplier);
        }
    }
}
