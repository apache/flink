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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DefaultExecutionPlanStore} with {@link TestingExecutionPlanStoreWatcher}, {@link
 * TestingStateHandleStore}, and {@link TestingExecutionPlanListener}.
 */
public class DefaultExecutionPlanStoreTest extends TestLogger {

    private final ExecutionPlan testingExecutionPlan = JobGraphTestUtils.emptyJobGraph();
    private final long timeout = 100L;

    private TestingStateHandleStore.Builder<ExecutionPlan> builder;
    private TestingRetrievableStateStorageHelper<ExecutionPlan> jobGraphStorageHelper;
    private TestingExecutionPlanStoreWatcher testingExecutionPlanStoreWatcher;
    private TestingExecutionPlanListener testingExecutionPlanListener;

    @Before
    public void setup() {
        builder = TestingStateHandleStore.newBuilder();
        testingExecutionPlanStoreWatcher = new TestingExecutionPlanStoreWatcher();
        testingExecutionPlanListener = new TestingExecutionPlanListener();
        jobGraphStorageHelper = new TestingRetrievableStateStorageHelper<>();
    }

    @After
    public void teardown() {
        if (testingExecutionPlanStoreWatcher != null) {
            testingExecutionPlanStoreWatcher.stop();
        }
    }

    @Test
    public void testRecoverExecutionPlan() throws Exception {
        final RetrievableStateHandle<ExecutionPlan> stateHandle =
                jobGraphStorageHelper.store(testingExecutionPlan);
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle).build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);

        final ExecutionPlan recoveredExecutionPlan =
                executionPlanStore.recoverExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(recoveredExecutionPlan).isNotNull();
        assertThat(recoveredExecutionPlan.getJobID()).isEqualTo(testingExecutionPlan.getJobID());
    }

    @Test
    public void testRecoverExecutionPlanWhenNotExist() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException(
                                            "Not exist exception.");
                                })
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);

        final ExecutionPlan recoveredExecutionPlan =
                executionPlanStore.recoverExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(recoveredExecutionPlan).isNull();
    }

    @Test
    public void testRecoverExecutionPlanFailedShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final FlinkException testException = new FlinkException("Test exception.");
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw testException;
                                })
                        .setReleaseConsumer(releaseFuture::complete)
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);

        assertThatThrownBy(
                        () ->
                                executionPlanStore.recoverExecutionPlan(
                                        testingExecutionPlan.getJobID()))
                .hasCause(testException);
        String actual = releaseFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(testingExecutionPlan.getJobID()).hasToString(actual);
    }

    @Test
    public void testPutExecutionPlanWhenNotExist() throws Exception {
        final CompletableFuture<ExecutionPlan> addFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setExistsFunction(ignore -> IntegerResourceVersion.notExisting())
                        .setAddFunction(
                                (ignore, state) -> {
                                    addFuture.complete(state);
                                    return jobGraphStorageHelper.store(state);
                                })
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);

        final ExecutionPlan actual = addFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.getJobID()).isEqualTo(testingExecutionPlan.getJobID());
    }

    @Test
    public void testPutExecutionPlanWhenAlreadyExist() throws Exception {
        final CompletableFuture<Tuple3<String, IntegerResourceVersion, ExecutionPlan>>
                replaceFuture = new CompletableFuture<>();
        final int resourceVersion = 100;
        final AtomicBoolean alreadyExist = new AtomicBoolean(false);
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setExistsFunction(
                                ignore -> {
                                    if (alreadyExist.get()) {
                                        return IntegerResourceVersion.valueOf(resourceVersion);
                                    } else {
                                        alreadyExist.set(true);
                                        return IntegerResourceVersion.notExisting();
                                    }
                                })
                        .setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .setReplaceConsumer(replaceFuture::complete)
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);
        // Replace
        executionPlanStore.putExecutionPlan(testingExecutionPlan);

        final Tuple3<String, IntegerResourceVersion, ExecutionPlan> actual =
                replaceFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.f0).isEqualTo(testingExecutionPlan.getJobID().toString());
        assertThat(actual.f1).isEqualTo(IntegerResourceVersion.valueOf(resourceVersion));
        assertThat(actual.f2.getJobID()).isEqualTo(testingExecutionPlan.getJobID());
    }

    @Test
    public void testGlobalCleanup() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);

        executionPlanStore.putExecutionPlan(testingExecutionPlan);
        executionPlanStore
                .globalCleanupAsync(testingExecutionPlan.getJobID(), Executors.directExecutor())
                .join();
        final JobID actual = removeFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual).isEqualTo(testingExecutionPlan.getJobID());
    }

    @Test
    public void testGlobalCleanupWithNonExistName() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore
                .globalCleanupAsync(testingExecutionPlan.getJobID(), Executors.directExecutor())
                .join();

        assertThat(removeFuture).isDone();
    }

    @Test
    public void testGlobalCleanupFailsIfRemovalReturnsFalse() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setRemoveFunction(name -> false).build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        assertThatThrownBy(
                        () ->
                                executionPlanStore
                                        .globalCleanupAsync(
                                                testingExecutionPlan.getJobID(),
                                                Executors.directExecutor())
                                        .get())
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    public void testGetJobIds() throws Exception {
        final List<JobID> existingJobIds = Arrays.asList(new JobID(0, 0), new JobID(0, 1));
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetAllHandlesSupplier(
                                () ->
                                        existingJobIds.stream()
                                                .map(AbstractID::toString)
                                                .collect(Collectors.toList()))
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        final Collection<JobID> jobIds = executionPlanStore.getJobIds();
        assertThat(jobIds).containsAll(existingJobIds);
    }

    @Test
    public void testOnAddedExecutionPlanShouldNotProcessKnownExecutionPlans() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);

        testingExecutionPlanStoreWatcher.addExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans()).isEmpty();
    }

    @Test
    public void testOnAddedExecutionPlanShouldOnlyProcessUnknownExecutionPlans() throws Exception {
        final RetrievableStateHandle<ExecutionPlan> stateHandle =
                jobGraphStorageHelper.store(testingExecutionPlan);
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle)
                        .setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.recoverExecutionPlan(testingExecutionPlan.getJobID());

        // Known recovered job
        testingExecutionPlanStoreWatcher.addExecutionPlan(testingExecutionPlan.getJobID());
        // Unknown job
        final JobID unknownJobId = JobID.generate();
        testingExecutionPlanStoreWatcher.addExecutionPlan(unknownJobId);
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans()).hasSize(1);
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans()).contains(unknownJobId);
    }

    @Test
    public void testOnRemovedExecutionPlanShouldOnlyProcessKnownExecutionPlans() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);

        // Unknown job
        testingExecutionPlanStoreWatcher.removeExecutionPlan(JobID.generate());
        // Known job
        testingExecutionPlanStoreWatcher.removeExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans()).hasSize(1);
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans())
                .contains(testingExecutionPlan.getJobID());
    }

    @Test
    public void testOnRemovedExecutionPlanShouldNotProcessUnknownExecutionPlans() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        createAndStartExecutionPlanStore(stateHandleStore);

        testingExecutionPlanStoreWatcher.removeExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans()).isEmpty();
    }

    @Test
    public void testOnAddedExecutionPlanIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.stop();

        testingExecutionPlanStoreWatcher.addExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans()).isEmpty();
    }

    @Test
    public void testOnRemovedExecutionPlanIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);
        executionPlanStore.stop();

        testingExecutionPlanStoreWatcher.removeExecutionPlan(testingExecutionPlan.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans()).isEmpty();
    }

    @Test
    public void testStoppingExecutionPlanStoreShouldReleaseAllHandles() throws Exception {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setReleaseAllHandlesRunnable(() -> completableFuture.complete(null))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.stop();

        assertThat(completableFuture).isDone();
    }

    @Test
    public void testLocalCleanupShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setReleaseConsumer(releaseFuture::complete).build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);
        executionPlanStore
                .localCleanupAsync(testingExecutionPlan.getJobID(), Executors.directExecutor())
                .join();

        final String actual = releaseFuture.get();
        assertThat(testingExecutionPlan.getJobID()).hasToString(actual);
    }

    @Test
    public void testRecoverPersistedJobResourceRequirements() throws Exception {
        final Map<String, RetrievableStateHandle<ExecutionPlan>> handles = new HashMap<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction(
                                (key, state) -> {
                                    final RetrievableStateHandle<ExecutionPlan> handle =
                                            jobGraphStorageHelper.store(state);
                                    handles.put(key, handle);
                                    return handle;
                                })
                        .setGetFunction(
                                key -> {
                                    final RetrievableStateHandle<ExecutionPlan> handle =
                                            handles.get(key);
                                    if (handle != null) {
                                        return handle;
                                    }
                                    throw new StateHandleStore.NotExistException("Does not exist.");
                                })
                        .build();

        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1)
                        .build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingExecutionPlan);
        executionPlanStore.putJobResourceRequirements(
                testingExecutionPlan.getJobID(), jobResourceRequirements);

        assertStoredRequirementsAre(
                executionPlanStore, testingExecutionPlan.getJobID(), jobResourceRequirements);

        final JobResourceRequirements updatedJobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1)
                        .build();

        executionPlanStore.putJobResourceRequirements(
                testingExecutionPlan.getJobID(), updatedJobResourceRequirements);

        assertStoredRequirementsAre(
                executionPlanStore,
                testingExecutionPlan.getJobID(),
                updatedJobResourceRequirements);
    }

    private static void assertStoredRequirementsAre(
            ExecutionPlanStore executionPlanStore, JobID jobId, JobResourceRequirements expected)
            throws Exception {
        final Optional<JobResourceRequirements> maybeRecovered =
                JobResourceRequirements.readFromExecutionPlan(
                        Objects.requireNonNull(
                                (JobGraph) executionPlanStore.recoverExecutionPlan(jobId)));
        assertThat(maybeRecovered).get().isEqualTo(expected);
    }

    @Test
    public void testPutJobResourceRequirementsOfNonExistentJob() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException("Does not exist.");
                                })
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        assertThatThrownBy(
                        () ->
                                executionPlanStore.putJobResourceRequirements(
                                        new JobID(), JobResourceRequirements.empty()))
                .isInstanceOf(NoSuchElementException.class);
    }

    private ExecutionPlanStore createAndStartExecutionPlanStore(
            TestingStateHandleStore<ExecutionPlan> stateHandleStore) throws Exception {
        final ExecutionPlanStore executionPlanStore =
                new DefaultExecutionPlanStore<>(
                        stateHandleStore,
                        testingExecutionPlanStoreWatcher,
                        new ExecutionPlanStoreUtil() {
                            @Override
                            public String jobIDToName(JobID jobId) {
                                return jobId.toString();
                            }

                            @Override
                            public JobID nameToJobID(String name) {
                                return JobID.fromHexString(name);
                            }
                        });
        executionPlanStore.start(testingExecutionPlanListener);
        return executionPlanStore;
    }
}
