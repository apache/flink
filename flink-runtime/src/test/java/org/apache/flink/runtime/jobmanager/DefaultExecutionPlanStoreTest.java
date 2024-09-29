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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.checkpoint.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultExecutionPlanStore} with {@link TestingExecutionPlanStoreWatcher}, {@link
 * TestingStateHandleStore}, and {@link TestingExecutionPlanListener}.
 */
public class DefaultExecutionPlanStoreTest extends TestLogger {

    private final JobGraph testingJobGraph = JobGraphTestUtils.emptyJobGraph();
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
    public void testRecoverJobGraph() throws Exception {
        final RetrievableStateHandle<ExecutionPlan> stateHandle =
                jobGraphStorageHelper.store(testingJobGraph);
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle).build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);

        final ExecutionPlan recoveredExecutionPlan =
                executionPlanStore.recoverExecutionPlan(testingJobGraph.getJobID());
        assertThat(recoveredExecutionPlan, is(notNullValue()));
        assertThat(recoveredExecutionPlan.getJobID(), is(testingJobGraph.getJobID()));
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
                executionPlanStore.recoverExecutionPlan(testingJobGraph.getJobID());
        assertThat(recoveredExecutionPlan, is(nullValue()));
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

        try {
            executionPlanStore.recoverExecutionPlan(testingJobGraph.getJobID());
            fail(
                    "recoverExecutionPlan should fail when there is exception in getting the state handle.");
        } catch (Exception ex) {
            assertThat(ex, FlinkMatchers.containsCause(testException));
            String actual = releaseFuture.get(timeout, TimeUnit.MILLISECONDS);
            assertThat(actual, is(testingJobGraph.getJobID().toString()));
        }
    }

    @Test
    public void testPutJobGraphWhenNotExist() throws Exception {
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
        executionPlanStore.putExecutionPlan(testingJobGraph);

        final ExecutionPlan actual = addFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.getJobID(), is(testingJobGraph.getJobID()));
    }

    @Test
    public void testPutJobGraphWhenAlreadyExist() throws Exception {
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
        executionPlanStore.putExecutionPlan(testingJobGraph);
        // Replace
        executionPlanStore.putExecutionPlan(testingJobGraph);

        final Tuple3<String, IntegerResourceVersion, ExecutionPlan> actual =
                replaceFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.f0, is(testingJobGraph.getJobID().toString()));
        assertThat(actual.f1, is(IntegerResourceVersion.valueOf(resourceVersion)));
        assertThat(actual.f2.getJobID(), is(testingJobGraph.getJobID()));
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

        executionPlanStore.putExecutionPlan(testingJobGraph);
        executionPlanStore
                .globalCleanupAsync(testingJobGraph.getJobID(), Executors.directExecutor())
                .join();
        final JobID actual = removeFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual, is(testingJobGraph.getJobID()));
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
                .globalCleanupAsync(testingJobGraph.getJobID(), Executors.directExecutor())
                .join();

        assertThat(removeFuture.isDone(), is(true));
    }

    @Test
    public void testGlobalCleanupFailsIfRemovalReturnsFalse() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setRemoveFunction(name -> false).build();

        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        assertThrows(
                ExecutionException.class,
                () ->
                        executionPlanStore
                                .globalCleanupAsync(
                                        testingJobGraph.getJobID(), Executors.directExecutor())
                                .get());
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
        assertThat(jobIds, contains(existingJobIds.toArray()));
    }

    @Test
    public void testOnAddedJobGraphShouldNotProcessKnownJobGraphs() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingJobGraph);

        testingExecutionPlanStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans().size(), is(0));
    }

    @Test
    public void testOnAddedJobGraphShouldOnlyProcessUnknownJobGraphs() throws Exception {
        final RetrievableStateHandle<ExecutionPlan> stateHandle =
                jobGraphStorageHelper.store(testingJobGraph);
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle)
                        .setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.recoverExecutionPlan(testingJobGraph.getJobID());

        // Known recovered job
        testingExecutionPlanStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        // Unknown job
        final JobID unknownJobId = JobID.generate();
        testingExecutionPlanStoreWatcher.addJobGraph(unknownJobId);
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans().size(), is(1));
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans(), contains(unknownJobId));
    }

    @Test
    public void testOnRemovedJobGraphShouldOnlyProcessKnownJobGraphs() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingJobGraph);

        // Unknown job
        testingExecutionPlanStoreWatcher.removeJobGraph(JobID.generate());
        // Known job
        testingExecutionPlanStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans().size(), is(1));
        assertThat(
                testingExecutionPlanListener.getRemovedExecutionPlans(),
                contains(testingJobGraph.getJobID()));
    }

    @Test
    public void testOnRemovedJobGraphShouldNotProcessUnknownJobGraphs() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        createAndStartExecutionPlanStore(stateHandleStore);

        testingExecutionPlanStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans().size(), is(0));
    }

    @Test
    public void testOnAddedJobGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.stop();

        testingExecutionPlanStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        assertThat(testingExecutionPlanListener.getAddedExecutionPlans().size(), is(0));
    }

    @Test
    public void testOnRemovedJobGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingJobGraph);
        executionPlanStore.stop();

        testingExecutionPlanStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingExecutionPlanListener.getRemovedExecutionPlans().size(), is(0));
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

        assertThat(completableFuture.isDone(), is(true));
    }

    @Test
    public void testLocalCleanupShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ExecutionPlan> stateHandleStore =
                builder.setReleaseConsumer(releaseFuture::complete).build();
        final ExecutionPlanStore executionPlanStore =
                createAndStartExecutionPlanStore(stateHandleStore);
        executionPlanStore.putExecutionPlan(testingJobGraph);
        executionPlanStore
                .localCleanupAsync(testingJobGraph.getJobID(), Executors.directExecutor())
                .join();

        final String actual = releaseFuture.get();
        assertThat(actual, is(testingJobGraph.getJobID().toString()));
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
        executionPlanStore.putExecutionPlan(testingJobGraph);
        executionPlanStore.putJobResourceRequirements(
                testingJobGraph.getJobID(), jobResourceRequirements);

        assertStoredRequirementsAre(
                executionPlanStore, testingJobGraph.getJobID(), jobResourceRequirements);

        final JobResourceRequirements updatedJobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1)
                        .build();

        executionPlanStore.putJobResourceRequirements(
                testingJobGraph.getJobID(), updatedJobResourceRequirements);

        assertStoredRequirementsAre(
                executionPlanStore, testingJobGraph.getJobID(), updatedJobResourceRequirements);
    }

    private static void assertStoredRequirementsAre(
            ExecutionPlanStore executionPlanStore, JobID jobId, JobResourceRequirements expected)
            throws Exception {
        final Optional<JobResourceRequirements> maybeRecovered =
                JobResourceRequirements.readFromJobGraph(
                        Objects.requireNonNull(
                                (JobGraph) executionPlanStore.recoverExecutionPlan(jobId)));
        Assertions.assertThat(maybeRecovered).get().isEqualTo(expected);
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
        assertThrows(
                NoSuchElementException.class,
                () ->
                        executionPlanStore.putJobResourceRequirements(
                                new JobID(), JobResourceRequirements.empty()));
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
