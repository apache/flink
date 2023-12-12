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
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderservice.LeaderServices;
import org.apache.flink.runtime.persistentservice.PersistentServices;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link HighAvailabilityServicesImpl}. */
class HighAvailabilityServicesImplTest {

    /**
     * Tests that we first delete all pointers from the HA services before deleting the blobs. See
     * FLINK-22014 for more details.
     */
    @Test
    void testCloseAndCleanupAllDataDeletesBlobsAfterCleaningUpHAData() throws Exception {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);

        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        final TestingHaServices haServices =
                new TestingHaServices(
                        testingBlobStoreService,
                        closeOperations,
                        () -> closeOperations.offer(CloseOperations.HA_CLEANUP),
                        ignored -> {});

        haServices.closeWithOptionalClean(true);

        assertThat(closeOperations)
                .contains(
                        CloseOperations.HA_CLEANUP,
                        CloseOperations.HA_CLOSE,
                        CloseOperations.BLOB_CLEANUP);
    }

    /**
     * Tests that we don't delete the HA blobs if we could not clean up the pointers from the HA
     * services. See FLINK-22014 for more details.
     */
    @Test
    void testCloseAndCleanupAllDataDoesNotDeleteBlobsIfCleaningUpHADataFails() throws Exception {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);

        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        final TestingHaServices haServices =
                new TestingHaServices(
                        testingBlobStoreService,
                        closeOperations,
                        () -> {
                            throw new FlinkException("test exception");
                        },
                        ignored -> {});

        assertThatThrownBy(() -> haServices.closeWithOptionalClean(true))
                .isInstanceOf(FlinkException.class);
        assertThat(closeOperations).contains(CloseOperations.HA_CLOSE, CloseOperations.BLOB_CLOSE);
    }

    @Test
    void testCleanupJobData() throws Exception {
        final Queue<CloseOperations> closeOperations = new ArrayDeque<>(3);
        final TestingBlobStoreService testingBlobStoreService =
                new TestingBlobStoreService(closeOperations);

        JobID jobID = new JobID();
        CompletableFuture<JobID> jobCleanupFuture = new CompletableFuture<>();

        final TestingHaServices haServices =
                new TestingHaServices(
                        testingBlobStoreService,
                        closeOperations,
                        () -> {},
                        jobCleanupFuture::complete);

        haServices.globalCleanupAsync(jobID, Executors.directExecutor()).join();
        JobID jobIDCleaned = jobCleanupFuture.get();
        assertThat(jobIDCleaned).isEqualTo(jobID);
    }

    private enum CloseOperations {
        HA_CLEANUP,
        HA_CLOSE,
        BLOB_CLEANUP,
        BLOB_CLOSE,
    }

    private static final class TestingBlobStoreService implements BlobStoreService {

        private final Queue<CloseOperations> closeOperations;

        private TestingBlobStoreService(Queue<CloseOperations> closeOperations) {
            this.closeOperations = closeOperations;
        }

        @Override
        public void cleanupAllData() {
            closeOperations.offer(CloseOperations.BLOB_CLEANUP);
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

    private static final class TestingPersistentServices implements PersistentServices {
        private final BlobStoreService blobStoreService;
        private final JobResultStore jobResultStore;

        private TestingPersistentServices(
                BlobStoreService blobStoreService, JobResultStore jobResultStore) {
            this.jobResultStore = jobResultStore;
            this.blobStoreService = blobStoreService;
        }

        @Override
        public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobGraphStore getJobGraphStore() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobResultStore getJobResultStore() throws Exception {
            return jobResultStore;
        }

        @Override
        public BlobStore getBlobStore() throws IOException {
            return blobStoreService;
        }

        @Override
        public void cleanup() throws Exception {
            blobStoreService.cleanupAllData();
        }

        @Override
        public void close() throws Exception {
            blobStoreService.close();
        }
    }

    private static final class TestingLeaderServices implements LeaderServices {
        private final Queue<? super CloseOperations> closeOperations;
        private final RunnableWithException internalCleanupRunnable;
        private final ThrowingConsumer<JobID, Exception> internalJobCleanupConsumer;

        private TestingLeaderServices(
                Queue<? super CloseOperations> closeOperations,
                RunnableWithException internalCleanupRunnable,
                ThrowingConsumer<JobID, Exception> internalJobCleanupConsumer) {
            this.closeOperations = closeOperations;
            this.internalCleanupRunnable = internalCleanupRunnable;
            this.internalJobCleanupConsumer = internalJobCleanupConsumer;
        }

        @Override
        public LeaderRetrievalService getRestEndpointLeaderRetriever() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderElection getResourceManagerLeaderElection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderRetrievalService getResourceManagerLeaderRetriever() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderElection getDispatcherLeaderElection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderRetrievalService getDispatcherLeaderRetriever() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderRetrievalService getJobMasterLeaderRetriever(
                JobID jobID, String defaultJobManagerAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderElection getJobMasterLeaderElection(JobID jobID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaderElection getRestEndpointLeaderElection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception {
            closeOperations.offer(CloseOperations.HA_CLOSE);
        }

        @Override
        public void cleanupServices() throws Exception {
            internalCleanupRunnable.run();
        }

        @Override
        public void cleanupJobData(JobID jobID) throws Exception {
            internalJobCleanupConsumer.accept(jobID);
        }
    }

    private static final class TestingHaServices extends HighAvailabilityServicesImpl {
        private TestingHaServices(
                BlobStoreService blobStoreService,
                Queue<? super CloseOperations> closeOperations,
                RunnableWithException internalCleanupRunnable,
                ThrowingConsumer<JobID, Exception> internalJobCleanupConsumer) {
            super(
                    new TestingLeaderServices(
                            closeOperations, internalCleanupRunnable, internalJobCleanupConsumer),
                    new TestingPersistentServices(
                            blobStoreService,
                            TestingJobResultStore.builder()
                                    .withMarkResultAsCleanConsumer(
                                            ignoredJobId -> {
                                                throw new AssertionError(
                                                        "Marking the job as clean shouldn't happen in the HaServices cleanup");
                                            })
                                    .build()));
        }
    }
}
