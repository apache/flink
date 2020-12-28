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
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultJobGraphStore} with {@link TestingJobGraphStoreWatcher}, {@link
 * TestingStateHandleStore}, and {@link TestingJobGraphListener}.
 */
public class DefaultJobGraphStoreTest extends TestLogger {

    private final JobGraph testingJobGraph = new JobGraph();
    private final long timeout = 100L;

    private TestingStateHandleStore.Builder<JobGraph> builder;
    private TestingRetrievableStateStorageHelper<JobGraph> jobGraphStorageHelper;
    private TestingJobGraphStoreWatcher testingJobGraphStoreWatcher;
    private TestingJobGraphListener testingJobGraphListener;

    @Before
    public void setup() {
        builder = TestingStateHandleStore.builder();
        testingJobGraphStoreWatcher = new TestingJobGraphStoreWatcher();
        testingJobGraphListener = new TestingJobGraphListener();
        jobGraphStorageHelper = new TestingRetrievableStateStorageHelper<>();
    }

    @After
    public void teardown() {
        if (testingJobGraphStoreWatcher != null) {
            testingJobGraphStoreWatcher.stop();
        }
    }

    @Test
    public void testRecoverJobGraph() throws Exception {
        final RetrievableStateHandle<JobGraph> stateHandle =
                jobGraphStorageHelper.store(testingJobGraph);
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle).build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);

        final JobGraph recoveredJobGraph =
                jobGraphStore.recoverJobGraph(testingJobGraph.getJobID());
        assertThat(recoveredJobGraph, is(notNullValue()));
        assertThat(recoveredJobGraph.getJobID(), is(testingJobGraph.getJobID()));
    }

    @Test
    public void testRecoverJobGraphWhenNotExist() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException(
                                            "Not exist exception.");
                                })
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);

        final JobGraph recoveredJobGraph =
                jobGraphStore.recoverJobGraph(testingJobGraph.getJobID());
        assertThat(recoveredJobGraph, is(nullValue()));
    }

    @Test
    public void testRecoverJobGraphFailedShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final FlinkException testException = new FlinkException("Test exception.");
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw testException;
                                })
                        .setReleaseConsumer(releaseFuture::complete)
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);

        try {
            jobGraphStore.recoverJobGraph(testingJobGraph.getJobID());
            fail(
                    "recoverJobGraph should fail when there is exception in getting the state handle.");
        } catch (Exception ex) {
            assertThat(ex, FlinkMatchers.containsCause(testException));
            String actual = releaseFuture.get(timeout, TimeUnit.MILLISECONDS);
            assertThat(actual, is(testingJobGraph.getJobID().toString()));
        }
    }

    @Test
    public void testPutJobGraphWhenNotExist() throws Exception {
        final CompletableFuture<JobGraph> addFuture = new CompletableFuture<>();
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setExistsFunction(ignore -> IntegerResourceVersion.notExisting())
                        .setAddFunction(
                                (ignore, state) -> {
                                    addFuture.complete(state);
                                    return jobGraphStorageHelper.store(state);
                                })
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);

        final JobGraph actual = addFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.getJobID(), is(testingJobGraph.getJobID()));
    }

    @Test
    public void testPutJobGraphWhenAlreadyExist() throws Exception {
        final CompletableFuture<Tuple3<String, IntegerResourceVersion, JobGraph>> replaceFuture =
                new CompletableFuture<>();
        final int resourceVersion = 100;
        final AtomicBoolean alreadyExist = new AtomicBoolean(false);
        final TestingStateHandleStore<JobGraph> stateHandleStore =
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

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);
        // Replace
        jobGraphStore.putJobGraph(testingJobGraph);

        final Tuple3<String, IntegerResourceVersion, JobGraph> actual =
                replaceFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.f0, is(testingJobGraph.getJobID().toString()));
        assertThat(actual.f1, is(IntegerResourceVersion.valueOf(resourceVersion)));
        assertThat(actual.f2.getJobID(), is(testingJobGraph.getJobID()));
    }

    @Test
    public void testRemoveJobGraph() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);

        jobGraphStore.putJobGraph(testingJobGraph);
        jobGraphStore.removeJobGraph(testingJobGraph.getJobID());
        final JobID actual = removeFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual, is(testingJobGraph.getJobID()));
    }

    @Test
    public void testRemoveJobGraphWithNonExistName() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.removeJobGraph(testingJobGraph.getJobID());

        try {
            removeFuture.get(timeout, TimeUnit.MILLISECONDS);
            fail(
                    "We should get an expected timeout because we are removing a non-existed job graph.");
        } catch (TimeoutException ex) {
            // expected
        }
        assertThat(removeFuture.isDone(), is(false));
    }

    @Test
    public void testGetJobIds() throws Exception {
        final List<JobID> existingJobIds = Arrays.asList(new JobID(0, 0), new JobID(0, 1));
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setGetAllHandlesSupplier(
                                () ->
                                        existingJobIds.stream()
                                                .map(AbstractID::toString)
                                                .collect(Collectors.toList()))
                        .build();

        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        final Collection<JobID> jobIds = jobGraphStore.getJobIds();
        assertThat(jobIds, contains(existingJobIds.toArray()));
    }

    @Test
    public void testOnAddedJobGraphShouldNotProcessKnownJobGraphs() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);

        testingJobGraphStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        assertThat(testingJobGraphListener.getAddedJobGraphs().size(), is(0));
    }

    @Test
    public void testOnAddedJobGraphShouldOnlyProcessUnknownJobGraphs() throws Exception {
        final RetrievableStateHandle<JobGraph> stateHandle =
                jobGraphStorageHelper.store(testingJobGraph);
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle)
                        .setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.recoverJobGraph(testingJobGraph.getJobID());

        // Known recovered job
        testingJobGraphStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        // Unknown job
        final JobID unknownJobId = JobID.generate();
        testingJobGraphStoreWatcher.addJobGraph(unknownJobId);
        assertThat(testingJobGraphListener.getAddedJobGraphs().size(), is(1));
        assertThat(testingJobGraphListener.getAddedJobGraphs(), contains(unknownJobId));
    }

    @Test
    public void testOnRemovedJobGraphShouldOnlyProcessKnownJobGraphs() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);

        // Unknown job
        testingJobGraphStoreWatcher.removeJobGraph(JobID.generate());
        // Known job
        testingJobGraphStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingJobGraphListener.getRemovedJobGraphs().size(), is(1));
        assertThat(
                testingJobGraphListener.getRemovedJobGraphs(),
                contains(testingJobGraph.getJobID()));
    }

    @Test
    public void testOnRemovedJobGraphShouldNotProcessUnknownJobGraphs() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        createAndStartJobGraphStore(stateHandleStore);

        testingJobGraphStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingJobGraphListener.getRemovedJobGraphs().size(), is(0));
    }

    @Test
    public void testOnAddedJobGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.stop();

        testingJobGraphStoreWatcher.addJobGraph(testingJobGraph.getJobID());
        assertThat(testingJobGraphListener.getAddedJobGraphs().size(), is(0));
    }

    @Test
    public void testOnRemovedJobGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> jobGraphStorageHelper.store(state))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);
        jobGraphStore.stop();

        testingJobGraphStoreWatcher.removeJobGraph(testingJobGraph.getJobID());
        assertThat(testingJobGraphListener.getRemovedJobGraphs().size(), is(0));
    }

    @Test
    public void testStoppingJobGraphStoreShouldReleaseAllHandles() throws Exception {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setReleaseAllHandlesRunnable(() -> completableFuture.complete(null))
                        .build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.stop();

        assertThat(completableFuture.isDone(), is(true));
    }

    @Test
    public void testReleasingJobGraphShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final TestingStateHandleStore<JobGraph> stateHandleStore =
                builder.setReleaseConsumer(releaseFuture::complete).build();
        final JobGraphStore jobGraphStore = createAndStartJobGraphStore(stateHandleStore);
        jobGraphStore.putJobGraph(testingJobGraph);
        jobGraphStore.releaseJobGraph(testingJobGraph.getJobID());

        final String actual = releaseFuture.get();
        assertThat(actual, is(testingJobGraph.getJobID().toString()));
    }

    private JobGraphStore createAndStartJobGraphStore(
            TestingStateHandleStore<JobGraph> stateHandleStore) throws Exception {
        final JobGraphStore jobGraphStore =
                new DefaultJobGraphStore<>(
                        stateHandleStore,
                        testingJobGraphStoreWatcher,
                        new JobGraphStoreUtil() {
                            @Override
                            public String jobIDToName(JobID jobId) {
                                return jobId.toString();
                            }

                            @Override
                            public JobID nameToJobID(String name) {
                                return JobID.fromHexString(name);
                            }
                        });
        jobGraphStore.start(testingJobGraphListener);
        return jobGraphStore;
    }
}
