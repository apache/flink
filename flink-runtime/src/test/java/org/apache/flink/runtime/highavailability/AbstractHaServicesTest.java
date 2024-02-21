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
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
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
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link AbstractHaServices}. */
class AbstractHaServicesTest {

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
                        new Configuration(),
                        Executors.directExecutor(),
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
                        new Configuration(),
                        Executors.directExecutor(),
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
                        new Configuration(),
                        Executors.directExecutor(),
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

    private static final class TestingHaServices extends AbstractHaServices {

        private final Queue<? super CloseOperations> closeOperations;
        private final RunnableWithException internalCleanupRunnable;
        private final ThrowingConsumer<JobID, Exception> internalJobCleanupConsumer;

        private TestingHaServices(
                Configuration config,
                Executor ioExecutor,
                BlobStoreService blobStoreService,
                Queue<? super CloseOperations> closeOperations,
                RunnableWithException internalCleanupRunnable,
                ThrowingConsumer<JobID, Exception> internalJobCleanupConsumer) {
            super(
                    config,
                    listener -> null,
                    ioExecutor,
                    blobStoreService,
                    TestingJobResultStore.builder()
                            .withMarkResultAsCleanConsumer(
                                    ignoredJobId -> {
                                        throw new AssertionError(
                                                "Marking the job as clean shouldn't happen in the HaServices cleanup");
                                    })
                            .build());
            this.closeOperations = closeOperations;
            this.internalCleanupRunnable = internalCleanupRunnable;
            this.internalJobCleanupConsumer = internalJobCleanupConsumer;
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
        protected String getLeaderPathForResourceManager() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderPathForDispatcher() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderPathForJobManager(JobID jobID) {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }

        @Override
        protected String getLeaderPathForRestServer() {
            throw new UnsupportedOperationException("Not supported by this test implementation.");
        }
    }
}
