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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.dispatcher.NoOpExecutionPlanListener;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * IT tests for {@link DefaultExecutionPlanStore} with all ZooKeeper components(e.g. {@link
 * ZooKeeperStateHandleStore}, {@link ZooKeeperExecutionPlanStoreWatcher}, {@link
 * ZooKeeperExecutionPlanStoreUtil}).
 */
public class ZooKeeperExecutionPlansStoreITCase extends TestLogger {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private static final RetrievableStateStorageHelper<ExecutionPlan> localStateStorage =
            executionPlan -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(UUID.randomUUID()),
                                InstantiationUtil.serializeObject(executionPlan));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

    @Test
    public void testPutAndRemoveExecutionPlan() throws Exception {
        ExecutionPlanStore executionPlans =
                createZooKeeperExecutionPlanStore("/testPutAndRemoveExecutionPlan");

        try {
            ExecutionPlanStore.ExecutionPlanListener listener =
                    mock(ExecutionPlanStore.ExecutionPlanListener.class);

            executionPlans.start(listener);

            ExecutionPlan executionPlan = createExecutionPlan(new JobID(), "JobName");

            // Empty state
            assertThat(executionPlans.getJobIds()).isEmpty();

            // Add initial
            executionPlans.putExecutionPlan(executionPlan);

            // Verify initial job graph
            Collection<JobID> jobIds = executionPlans.getJobIds();
            assertThat(jobIds).hasSize(1);

            JobID jobId = jobIds.iterator().next();

            verifyExecutionPlans(executionPlan, executionPlans.recoverExecutionPlan(jobId));

            // Update (same ID)
            executionPlan = createExecutionPlan(executionPlan.getJobID(), "Updated JobName");
            executionPlans.putExecutionPlan(executionPlan);

            // Verify updated
            jobIds = executionPlans.getJobIds();
            assertThat(jobIds).hasSize(1);

            jobId = jobIds.iterator().next();

            verifyExecutionPlans(executionPlan, executionPlans.recoverExecutionPlan(jobId));

            // Remove
            executionPlans
                    .globalCleanupAsync(executionPlan.getJobID(), Executors.directExecutor())
                    .join();

            // Empty state
            assertThat(executionPlans.getJobIds()).isEmpty();

            // Nothing should have been notified
            verify(listener, atMost(1)).onAddedExecutionPlan(any(JobID.class));
            verify(listener, never()).onRemovedExecutionPlan(any(JobID.class));

            // Don't fail if called again
            executionPlans
                    .globalCleanupAsync(executionPlan.getJobID(), Executors.directExecutor())
                    .join();
        } finally {
            executionPlans.stop();
        }
    }

    @Nonnull
    private ExecutionPlanStore createZooKeeperExecutionPlanStore(String fullPath) throws Exception {
        final CuratorFramework client =
                zooKeeperExtension.getZooKeeperClient(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(fullPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + fullPath);
        final ZooKeeperStateHandleStore<ExecutionPlan> zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, localStateStorage);
        return new DefaultExecutionPlanStore<>(
                zooKeeperStateHandleStore,
                new ZooKeeperExecutionPlanStoreWatcher(new PathChildrenCache(facade, "/", false)),
                ZooKeeperExecutionPlanStoreUtil.INSTANCE);
    }

    @Test
    public void testRecoverExecutionPlans() throws Exception {
        ExecutionPlanStore executionPlans =
                createZooKeeperExecutionPlanStore("/testRecoverExecutionPlans");

        try {
            ExecutionPlanStore.ExecutionPlanListener listener =
                    mock(ExecutionPlanStore.ExecutionPlanListener.class);

            executionPlans.start(listener);

            HashMap<JobID, ExecutionPlan> expected = new HashMap<>();
            JobID[] jobIds = new JobID[] {new JobID(), new JobID(), new JobID()};

            expected.put(jobIds[0], createExecutionPlan(jobIds[0]));
            expected.put(jobIds[1], createExecutionPlan(jobIds[1]));
            expected.put(jobIds[2], createExecutionPlan(jobIds[2]));

            // Add all
            for (ExecutionPlan executionPlan : expected.values()) {
                executionPlans.putExecutionPlan(executionPlan);
            }

            Collection<JobID> actual = executionPlans.getJobIds();

            assertThat(actual).hasSameSizeAs(expected.entrySet());

            for (JobID jobId : actual) {
                ExecutionPlan executionPlan = executionPlans.recoverExecutionPlan(jobId);
                assertThat(expected).containsKey(executionPlan.getJobID());

                verifyExecutionPlans(expected.get(executionPlan.getJobID()), executionPlan);

                executionPlans
                        .globalCleanupAsync(executionPlan.getJobID(), Executors.directExecutor())
                        .join();
            }

            // Empty state
            assertThat(executionPlans.getJobIds()).isEmpty();

            // Nothing should have been notified
            verify(listener, atMost(expected.size())).onAddedExecutionPlan(any(JobID.class));
            verify(listener, never()).onRemovedExecutionPlan(any(JobID.class));
        } finally {
            executionPlans.stop();
        }
    }

    @Test
    public void testConcurrentAddExecutionPlan() throws Exception {
        ExecutionPlanStore executionPlans = null;
        ExecutionPlanStore otherExecutionPlans = null;

        try {
            executionPlans = createZooKeeperExecutionPlanStore("/testConcurrentAddExecutionPlan");

            otherExecutionPlans =
                    createZooKeeperExecutionPlanStore("/testConcurrentAddExecutionPlan");

            ExecutionPlan executionPlan = createExecutionPlan(new JobID());
            ExecutionPlan otherExecutionPlan = createExecutionPlan(new JobID());

            ExecutionPlanStore.ExecutionPlanListener listener =
                    mock(ExecutionPlanStore.ExecutionPlanListener.class);

            final JobID[] actualOtherJobId = new JobID[1];
            final CountDownLatch sync = new CountDownLatch(1);

            doAnswer(
                            new Answer<Void>() {
                                @Override
                                public Void answer(InvocationOnMock invocation) throws Throwable {
                                    actualOtherJobId[0] = (JobID) invocation.getArguments()[0];
                                    sync.countDown();

                                    return null;
                                }
                            })
                    .when(listener)
                    .onAddedExecutionPlan(any(JobID.class));

            // Test
            executionPlans.start(listener);
            otherExecutionPlans.start(NoOpExecutionPlanListener.INSTANCE);

            executionPlans.putExecutionPlan(executionPlan);

            // Everything is cool... not much happening ;)
            verify(listener, never()).onAddedExecutionPlan(any(JobID.class));
            verify(listener, never()).onRemovedExecutionPlan(any(JobID.class));

            // This bad boy adds the other job graph
            otherExecutionPlans.putExecutionPlan(otherExecutionPlan);

            // Wait for the cache to call back
            sync.await();

            verify(listener, times(1)).onAddedExecutionPlan(any(JobID.class));
            verify(listener, never()).onRemovedExecutionPlan(any(JobID.class));

            assertThat(actualOtherJobId[0]).isEqualTo(otherExecutionPlan.getJobID());
        } finally {
            if (executionPlans != null) {
                executionPlans.stop();
            }

            if (otherExecutionPlans != null) {
                otherExecutionPlans.stop();
            }
        }
    }

    @Test
    public void testUpdateExecutionPlanYouDidNotGetOrAdd() throws Exception {
        ExecutionPlanStore executionPlans =
                createZooKeeperExecutionPlanStore("/testUpdateExecutionPlanYouDidNotGetOrAdd");

        ExecutionPlanStore otherExecutionPlans =
                createZooKeeperExecutionPlanStore("/testUpdateExecutionPlanYouDidNotGetOrAdd");

        executionPlans.start(NoOpExecutionPlanListener.INSTANCE);
        otherExecutionPlans.start(NoOpExecutionPlanListener.INSTANCE);

        ExecutionPlan executionPlan = createExecutionPlan(new JobID());

        executionPlans.putExecutionPlan(executionPlan);

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> otherExecutionPlans.putExecutionPlan(executionPlan));
    }

    /**
     * Tests that we fail with an exception if the job cannot be removed from the
     * ZooKeeperExecutionPlanStore.
     *
     * <p>Tests that a close ZooKeeperExecutionPlanStore no longer holds any locks.
     */
    @Test
    public void testExecutionPlanRemovalFailureAndLockRelease() throws Exception {
        final ExecutionPlanStore submittedExecutionPlanStore =
                createZooKeeperExecutionPlanStore("/testConcurrentAddExecutionPlan");
        final ExecutionPlanStore otherSubmittedExecutionPlanStore =
                createZooKeeperExecutionPlanStore("/testConcurrentAddExecutionPlan");

        final TestingExecutionPlanListener listener = new TestingExecutionPlanListener();
        submittedExecutionPlanStore.start(listener);
        otherSubmittedExecutionPlanStore.start(listener);

        final ExecutionPlan executionPlan = JobGraphTestUtils.emptyJobGraph();
        submittedExecutionPlanStore.putExecutionPlan(executionPlan);

        final ExecutionPlan recoveredExecutionPlan =
                otherSubmittedExecutionPlanStore.recoverExecutionPlan(executionPlan.getJobID());

        assertThat(recoveredExecutionPlan).isNotNull();

        assertThatExceptionOfType(Exception.class)
                .as(
                        "It should not be possible to remove the ExecutionPlan since the first store still has a lock on it.")
                .isThrownBy(
                        () ->
                                otherSubmittedExecutionPlanStore
                                        .globalCleanupAsync(
                                                recoveredExecutionPlan.getJobID(),
                                                Executors.directExecutor())
                                        .join());

        submittedExecutionPlanStore.stop();

        // now we should be able to delete the job graph
        otherSubmittedExecutionPlanStore
                .globalCleanupAsync(recoveredExecutionPlan.getJobID(), Executors.directExecutor())
                .join();

        assertThat(
                        otherSubmittedExecutionPlanStore.recoverExecutionPlan(
                                recoveredExecutionPlan.getJobID()))
                .isNull();

        otherSubmittedExecutionPlanStore.stop();
    }

    // ---------------------------------------------------------------------------------------------

    private ExecutionPlan createExecutionPlan(JobID jobId) {
        return createExecutionPlan(jobId, "Test ExecutionPlan");
    }

    private ExecutionPlan createExecutionPlan(JobID jobId, String jobName) {
        final JobVertex jobVertex = new JobVertex("Test JobVertex");
        jobVertex.setParallelism(1);

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .setJobName(jobName)
                .setJobId(jobId)
                .addJobVertex(jobVertex)
                .build();
    }

    private void verifyExecutionPlans(ExecutionPlan expected, ExecutionPlan actual) {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getJobID()).isEqualTo(expected.getJobID());
    }
}
