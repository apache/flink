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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStore;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/** In-Memory implementation of {@link ExecutionPlanStore} for testing purposes. */
public class TestingExecutionPlanStore implements ExecutionPlanStore {

    private final Map<JobID, ExecutionPlan> storedJobs = new HashMap<>();

    private final ThrowingConsumer<ExecutionPlanListener, ? extends Exception> startConsumer;

    private final ThrowingRunnable<? extends Exception> stopRunnable;

    private final FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
            jobIdsFunction;

    private final BiFunctionWithException<
                    JobID, Map<JobID, ExecutionPlan>, ExecutionPlan, ? extends Exception>
            recoverExecutionPlanFunction;

    private final ThrowingConsumer<ExecutionPlan, ? extends Exception> putExecutionPlanConsumer;

    private final BiConsumerWithException<
                    ExecutionPlan, JobResourceRequirements, ? extends Exception>
            putJobResourceRequirementsConsumer;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction;

    private boolean started;

    private TestingExecutionPlanStore(
            ThrowingConsumer<ExecutionPlanListener, ? extends Exception> startConsumer,
            ThrowingRunnable<? extends Exception> stopRunnable,
            FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                    jobIdsFunction,
            BiFunctionWithException<
                            JobID, Map<JobID, ExecutionPlan>, ExecutionPlan, ? extends Exception>
                    recoverExecutionPlanFunction,
            ThrowingConsumer<ExecutionPlan, ? extends Exception> putExecutionPlanConsumer,
            BiConsumerWithException<ExecutionPlan, JobResourceRequirements, ? extends Exception>
                    putJobResourceRequirementsConsumer,
            BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction,
            BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction,
            Collection<ExecutionPlan> initialExecutionPlans) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.jobIdsFunction = jobIdsFunction;
        this.recoverExecutionPlanFunction = recoverExecutionPlanFunction;
        this.putExecutionPlanConsumer = putExecutionPlanConsumer;
        this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
        this.globalCleanupFunction = globalCleanupFunction;
        this.localCleanupFunction = localCleanupFunction;

        for (ExecutionPlan initialExecutionPlan : initialExecutionPlans) {
            storedJobs.put(initialExecutionPlan.getJobID(), initialExecutionPlan);
        }
    }

    @Override
    public synchronized void start(@Nullable ExecutionPlanListener executionPlanListener)
            throws Exception {
        startConsumer.accept(executionPlanListener);
        started = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        stopRunnable.run();
        started = false;
    }

    @Override
    public synchronized ExecutionPlan recoverExecutionPlan(JobID jobId) throws Exception {
        verifyIsStarted();
        return recoverExecutionPlanFunction.apply(jobId, storedJobs);
    }

    @Override
    public synchronized void putExecutionPlan(ExecutionPlan executionPlan) throws Exception {
        verifyIsStarted();
        putExecutionPlanConsumer.accept(executionPlan);
        storedJobs.put(executionPlan.getJobID(), executionPlan);
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        verifyIsStarted();
        final ExecutionPlan executionPlan =
                Preconditions.checkNotNull(storedJobs.get(jobId), "Job [%s] not found.", jobId);
        putJobResourceRequirementsConsumer.accept(executionPlan, jobResourceRequirements);
    }

    @Override
    public synchronized CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return globalCleanupFunction.apply(jobId, executor).thenRun(() -> storedJobs.remove(jobId));
    }

    @Override
    public synchronized CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return localCleanupFunction.apply(jobId, executor);
    }

    @Override
    public synchronized Collection<JobID> getJobIds() throws Exception {
        verifyIsStarted();
        return jobIdsFunction.apply(
                Collections.unmodifiableSet(new HashSet<>(storedJobs.keySet())));
    }

    public synchronized boolean contains(JobID jobId) {
        return storedJobs.containsKey(jobId);
    }

    private void verifyIsStarted() {
        Preconditions.checkState(started, "Not running. Forgot to call start()?");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingExecutionPlanStore} instances. */
    public static class Builder {
        private ThrowingConsumer<ExecutionPlanListener, ? extends Exception> startConsumer =
                ignored -> {};

        private ThrowingRunnable<? extends Exception> stopRunnable = () -> {};

        private FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                jobIdsFunction = jobIds -> jobIds;

        private BiFunctionWithException<
                        JobID, Map<JobID, ExecutionPlan>, ExecutionPlan, ? extends Exception>
                recoverExecutionPlanFunction = (jobId, jobs) -> jobs.get(jobId);

        private ThrowingConsumer<ExecutionPlan, ? extends Exception> putExecutionPlanConsumer =
                ignored -> {};

        private BiConsumerWithException<ExecutionPlan, JobResourceRequirements, ? extends Exception>
                putJobResourceRequirementsConsumer = (graph, requirements) -> {};

        private BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private Collection<ExecutionPlan> initialExecutionPlans = Collections.emptyList();

        private boolean startExecutionPlanStore = false;

        private Builder() {}

        public Builder setStartConsumer(
                ThrowingConsumer<ExecutionPlanListener, ? extends Exception> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(ThrowingRunnable<? extends Exception> stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setJobIdsFunction(
                FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                        jobIdsFunction) {
            this.jobIdsFunction = jobIdsFunction;
            return this;
        }

        public Builder setRecoverExecutionPlanFunction(
                BiFunctionWithException<
                                JobID,
                                Map<JobID, ExecutionPlan>,
                                ExecutionPlan,
                                ? extends Exception>
                        recoverExecutionPlanFunction) {
            this.recoverExecutionPlanFunction = recoverExecutionPlanFunction;
            return this;
        }

        public Builder setPutExecutionPlanConsumer(
                ThrowingConsumer<ExecutionPlan, ? extends Exception> putExecutionPlanConsumer) {
            this.putExecutionPlanConsumer = putExecutionPlanConsumer;
            return this;
        }

        public Builder setPutJobResourceRequirementsConsumer(
                BiConsumerWithException<ExecutionPlan, JobResourceRequirements, ? extends Exception>
                        putJobResourceRequirementsConsumer) {
            this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
            return this;
        }

        public Builder setGlobalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction) {
            this.globalCleanupFunction = globalCleanupFunction;
            return this;
        }

        public Builder setLocalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction) {
            this.localCleanupFunction = localCleanupFunction;
            return this;
        }

        public Builder setInitialExecutionPlans(Collection<ExecutionPlan> initialExecutionPlans) {
            this.initialExecutionPlans = initialExecutionPlans;
            return this;
        }

        public Builder withAutomaticStart() {
            this.startExecutionPlanStore = true;
            return this;
        }

        public TestingExecutionPlanStore build() {
            final TestingExecutionPlanStore executionPlanStore =
                    new TestingExecutionPlanStore(
                            startConsumer,
                            stopRunnable,
                            jobIdsFunction,
                            recoverExecutionPlanFunction,
                            putExecutionPlanConsumer,
                            putJobResourceRequirementsConsumer,
                            globalCleanupFunction,
                            localCleanupFunction,
                            initialExecutionPlans);

            if (startExecutionPlanStore) {
                try {
                    executionPlanStore.start(null);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            }

            return executionPlanStore;
        }
    }
}
