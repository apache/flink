/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link AbstractHaServices}. */
public class AbstractHaServicesTest extends TestLogger {

    /**
     * Tests that we first delete all pointers from the HA services before deleting the blobs. See
     * FLINK-22014 for more details.
     */
    @Test
    public void testCloseAndCleanupAllDataDeletesBlobsAfterCleaningUpHAData() throws Exception {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);

        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        final TestingHaServices haServices =
                new TestingHaServices(
                        new Configuration(),
                        Executors.directExecutor(),
                        testingBlobStoreService,
                        closeOperations,
                        () -> closeOperations.offer(CloseOperations.HA_CLEANUP),
                        ignored -> {});

        haServices.closeAndCleanupAllData();

        assertThat(
                closeOperations,
                contains(
                        CloseOperations.HA_CLEANUP,
                        CloseOperations.HA_CLOSE,
                        CloseOperations.BLOB_CLEANUP_AND_CLOSE));
    }

    /**
     * Tests that we don't delete the HA blobs if we could not clean up the pointers from the HA
     * services. See FLINK-22014 for more details.
     */
    @Test
    public void testCloseAndCleanupAllDataDoesNotDeleteBlobsIfCleaningUpHADataFails() {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);

        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        final TestingHaServices haServices =
                new TestingHaServices(
                        new Configuration(),
                        Executors.directExecutor(),
                        testingBlobStoreService,
                        closeOperations,
                        () -> {
                            throw new FlinkException("test exception");
                        },
                        ignored -> {});

        try {
            haServices.closeAndCleanupAllData();
            fail("Expected that the close operation fails.");
        } catch (Exception expected) {

        }

        assertThat(closeOperations, contains(CloseOperations.HA_CLOSE, CloseOperations.BLOB_CLOSE));
    }

    @Test
    public void testCleanupJobData() throws Exception {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);
        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        JobID jobID = new JobID();
        CompletableFuture<JobID> jobCleanupFuture = new CompletableFuture<>();

        final TestingHaServices haServices =
                new TestingHaServices(
                        new Configuration(),
                        Executors.directExecutor(),
                        testingBlobStoreService,
                        closeOperations,
                        () -> {},
                        jobCleanupFuture::complete);

        haServices.cleanupJobData(jobID);
        JobID jobIDCleaned = jobCleanupFuture.get();
        assertThat(jobIDCleaned, is(jobID));
    }

    private enum CloseOperations {
        HA_CLEANUP,
        HA_CLOSE,
        BLOB_CLEANUP_AND_CLOSE,
        BLOB_CLOSE,
    }

    private static final class TestingBlobStoreService implements BlobStoreService {

        private final Queue<CloseOperations> closeOperations;

        private TestingBlobStoreService(Queue<CloseOperations> closeOperations) {
            this.closeOperations = closeOperations;
        }

        @Override
        public void closeAndCleanupAllData() {
            closeOperations.offer(CloseOperations.BLOB_CLEANUP_AND_CLOSE);
        }

        @Override
        public void close() throws IOException {
            closeOperations.offer(CloseOperations.BLOB_CLOSE);
        }

        @Override
        public boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException {
            return false;
        }

        @Override
        public boolean delete(JobID jobId, BlobKey blobKey) {
            return false;
        }

        @Override
        public boolean deleteAll(JobID jobId) {
            return false;
        }

        @Override
        public boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException {
            return false;
        }
    }

    private static final class TestingHaServices extends AbstractHaServices {

        private final Queue<? super CloseOperations> closeOperations;
        private final RunnableWithException internalCleanupRunnable;
        private final Consumer<JobID> internalJobCleanupConsumer;

        private TestingHaServices(
                Configuration config,
                Executor ioExecutor,
                BlobStoreService blobStoreService,
                Queue<? super CloseOperations> closeOperations,
                RunnableWithException internalCleanupRunnable,
                Consumer<JobID> internalJobCleanupConsumer) {
            super(config, ioExecutor, blobStoreService);
            this.closeOperations = closeOperations;
            this.internalCleanupRunnable = internalCleanupRunnable;
            this.internalJobCleanupConsumer = internalJobCleanupConsumer;
        }

        @Override
        protected LeaderElectionService createLeaderElectionService(String leaderName) {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected LeaderRetrievalService createLeaderRetrievalService(String leaderName) {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected CheckpointRecoveryFactory createCheckpointRecoveryFactory() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected JobGraphStore createJobGraphStore() throws Exception {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected RunningJobsRegistry createRunningJobsRegistry() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected void internalClose() {
            closeOperations.offer(CloseOperations.HA_CLOSE);
        }

        @Override
        protected void internalCleanup() throws Exception {
            internalCleanupRunnable.run();
        }

        @Override
        protected void internalCleanupJobData(JobID jobID) throws Exception {
            internalJobCleanupConsumer.accept(jobID);
        }

        @Override
        protected String getLeaderNameForResourceManager() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderNameForDispatcher() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderNameForJobManager(JobID jobID) {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderNameForRestServer() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }
    }
}
