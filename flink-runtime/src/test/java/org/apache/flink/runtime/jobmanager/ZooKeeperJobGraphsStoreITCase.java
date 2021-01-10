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
import org.apache.flink.runtime.dispatcher.NoOpJobGraphListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphStore.JobGraphListener;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * IT tests for {@link DefaultJobGraphStore} with all ZooKeeper components(e.g. {@link
 * ZooKeeperStateHandleStore}, {@link ZooKeeperJobGraphStoreWatcher}, {@link
 * ZooKeeperJobGraphStoreUtil}).
 */
public class ZooKeeperJobGraphsStoreITCase extends TestLogger {

    private static final ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

    private static final RetrievableStateStorageHelper<JobGraph> localStateStorage =
            jobGraph -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(UUID.randomUUID()),
                                InstantiationUtil.serializeObject(jobGraph));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

    @AfterClass
    public static void tearDown() throws Exception {
        ZooKeeper.shutdown();
    }

    @Before
    public void cleanUp() throws Exception {
        ZooKeeper.deleteAll();
    }

    @Test
    public void testPutAndRemoveJobGraph() throws Exception {
        JobGraphStore jobGraphs = createZooKeeperJobGraphStore("/testPutAndRemoveJobGraph");

        try {
            JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

            jobGraphs.start(listener);

            JobGraph jobGraph = createJobGraph(new JobID(), "JobName");

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Add initial
            jobGraphs.putJobGraph(jobGraph);

            // Verify initial job graph
            Collection<JobID> jobIds = jobGraphs.getJobIds();
            assertEquals(1, jobIds.size());

            JobID jobId = jobIds.iterator().next();

            verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

            // Update (same ID)
            jobGraph = createJobGraph(jobGraph.getJobID(), "Updated JobName");
            jobGraphs.putJobGraph(jobGraph);

            // Verify updated
            jobIds = jobGraphs.getJobIds();
            assertEquals(1, jobIds.size());

            jobId = jobIds.iterator().next();

            verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

            // Remove
            jobGraphs.removeJobGraph(jobGraph.getJobID());

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Nothing should have been notified
            verify(listener, atMost(1)).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));

            // Don't fail if called again
            jobGraphs.removeJobGraph(jobGraph.getJobID());
        } finally {
            jobGraphs.stop();
        }
    }

    @Nonnull
    private JobGraphStore createZooKeeperJobGraphStore(String fullPath) throws Exception {
        final CuratorFramework client = ZooKeeper.getClient();
        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(fullPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + fullPath);
        final ZooKeeperStateHandleStore<JobGraph> zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, localStateStorage);
        return new DefaultJobGraphStore<>(
                zooKeeperStateHandleStore,
                new ZooKeeperJobGraphStoreWatcher(new PathChildrenCache(facade, "/", false)),
                ZooKeeperJobGraphStoreUtil.INSTANCE);
    }

    @Test
    public void testRecoverJobGraphs() throws Exception {
        JobGraphStore jobGraphs = createZooKeeperJobGraphStore("/testRecoverJobGraphs");

        try {
            JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

            jobGraphs.start(listener);

            HashMap<JobID, JobGraph> expected = new HashMap<>();
            JobID[] jobIds = new JobID[] {new JobID(), new JobID(), new JobID()};

            expected.put(jobIds[0], createJobGraph(jobIds[0]));
            expected.put(jobIds[1], createJobGraph(jobIds[1]));
            expected.put(jobIds[2], createJobGraph(jobIds[2]));

            // Add all
            for (JobGraph jobGraph : expected.values()) {
                jobGraphs.putJobGraph(jobGraph);
            }

            Collection<JobID> actual = jobGraphs.getJobIds();

            assertEquals(expected.size(), actual.size());

            for (JobID jobId : actual) {
                JobGraph jobGraph = jobGraphs.recoverJobGraph(jobId);
                assertTrue(expected.containsKey(jobGraph.getJobID()));

                verifyJobGraphs(expected.get(jobGraph.getJobID()), jobGraph);

                jobGraphs.removeJobGraph(jobGraph.getJobID());
            }

            // Empty state
            assertEquals(0, jobGraphs.getJobIds().size());

            // Nothing should have been notified
            verify(listener, atMost(expected.size())).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));
        } finally {
            jobGraphs.stop();
        }
    }

    @Test
    public void testConcurrentAddJobGraph() throws Exception {
        JobGraphStore jobGraphs = null;
        JobGraphStore otherJobGraphs = null;

        try {
            jobGraphs = createZooKeeperJobGraphStore("/testConcurrentAddJobGraph");

            otherJobGraphs = createZooKeeperJobGraphStore("/testConcurrentAddJobGraph");

            JobGraph jobGraph = createJobGraph(new JobID());
            JobGraph otherJobGraph = createJobGraph(new JobID());

            JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

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
                    .onAddedJobGraph(any(JobID.class));

            // Test
            jobGraphs.start(listener);
            otherJobGraphs.start(NoOpJobGraphListener.INSTANCE);

            jobGraphs.putJobGraph(jobGraph);

            // Everything is cool... not much happening ;)
            verify(listener, never()).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));

            // This bad boy adds the other job graph
            otherJobGraphs.putJobGraph(otherJobGraph);

            // Wait for the cache to call back
            sync.await();

            verify(listener, times(1)).onAddedJobGraph(any(JobID.class));
            verify(listener, never()).onRemovedJobGraph(any(JobID.class));

            assertEquals(otherJobGraph.getJobID(), actualOtherJobId[0]);
        } finally {
            if (jobGraphs != null) {
                jobGraphs.stop();
            }

            if (otherJobGraphs != null) {
                otherJobGraphs.stop();
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateJobGraphYouDidNotGetOrAdd() throws Exception {
        JobGraphStore jobGraphs =
                createZooKeeperJobGraphStore("/testUpdateJobGraphYouDidNotGetOrAdd");

        JobGraphStore otherJobGraphs =
                createZooKeeperJobGraphStore("/testUpdateJobGraphYouDidNotGetOrAdd");

        jobGraphs.start(NoOpJobGraphListener.INSTANCE);
        otherJobGraphs.start(NoOpJobGraphListener.INSTANCE);

        JobGraph jobGraph = createJobGraph(new JobID());

        jobGraphs.putJobGraph(jobGraph);

        otherJobGraphs.putJobGraph(jobGraph);
    }

    /**
     * Tests that we fail with an exception if the job cannot be removed from the
     * ZooKeeperJobGraphStore.
     *
     * <p>Tests that a close ZooKeeperJobGraphStore no longer holds any locks.
     */
    @Test
    public void testJobGraphRemovalFailureAndLockRelease() throws Exception {
        final JobGraphStore submittedJobGraphStore =
                createZooKeeperJobGraphStore("/testConcurrentAddJobGraph");
        final JobGraphStore otherSubmittedJobGraphStore =
                createZooKeeperJobGraphStore("/testConcurrentAddJobGraph");

        final TestingJobGraphListener listener = new TestingJobGraphListener();
        submittedJobGraphStore.start(listener);
        otherSubmittedJobGraphStore.start(listener);

        final JobGraph jobGraph = new JobGraph();
        submittedJobGraphStore.putJobGraph(jobGraph);

        final JobGraph recoveredJobGraph =
                otherSubmittedJobGraphStore.recoverJobGraph(jobGraph.getJobID());

        assertThat(recoveredJobGraph, is(notNullValue()));

        try {
            otherSubmittedJobGraphStore.removeJobGraph(recoveredJobGraph.getJobID());
            fail(
                    "It should not be possible to remove the JobGraph since the first store still has a lock on it.");
        } catch (Exception ignored) {
            // expected
        }

        submittedJobGraphStore.stop();

        // now we should be able to delete the job graph
        otherSubmittedJobGraphStore.removeJobGraph(recoveredJobGraph.getJobID());

        assertThat(
                otherSubmittedJobGraphStore.recoverJobGraph(recoveredJobGraph.getJobID()),
                is(nullValue()));

        otherSubmittedJobGraphStore.stop();
    }

    // ---------------------------------------------------------------------------------------------

    private JobGraph createJobGraph(JobID jobId) {
        return createJobGraph(jobId, "Test JobGraph");
    }

    private JobGraph createJobGraph(JobID jobId, String jobName) {
        final JobGraph jobGraph = new JobGraph(jobId, jobName);

        final JobVertex jobVertex = new JobVertex("Test JobVertex");
        jobVertex.setParallelism(1);

        jobGraph.addVertex(jobVertex);

        return jobGraph;
    }

    private void verifyJobGraphs(JobGraph expected, JobGraph actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getJobID(), actual.getJobID());
    }
}
