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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.FailingBlockingInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FilenameFilter;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Small test to check that the {@link org.apache.flink.runtime.blob.BlobServer} cleanup is executed
 * after job termination.
 */
public class BlobsCleanupITCase extends TestLogger {

    private static final long RETRY_INTERVAL = 100L;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static MiniClusterResource miniClusterResource;

    private static UnmodifiableConfiguration configuration;

    private static File blobBaseDir;

    @BeforeClass
    public static void setup() throws Exception {
        blobBaseDir = TEMPORARY_FOLDER.newFolder();

        Configuration cfg = new Configuration();
        cfg.setString(BlobServerOptions.STORAGE_DIRECTORY, blobBaseDir.getAbsolutePath());
        cfg.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixeddelay");
        cfg.setInteger(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        // BLOBs are deleted from BlobCache between 1s and 2s after last reference
        // -> the BlobCache may still have the BLOB or not (let's test both cases randomly)
        cfg.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

        configuration = new UnmodifiableConfiguration(cfg);

        miniClusterResource =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(2)
                                .setNumberTaskManagers(1)
                                .setConfiguration(configuration)
                                .build());

        miniClusterResource.before();
    }

    @AfterClass
    public static void teardown() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    /** Specifies which test case to run in {@link #testBlobServerCleanup(TestCase)}. */
    private enum TestCase {
        JOB_FINISHES_SUCESSFULLY,
        JOB_IS_CANCELLED,
        JOB_FAILS,
        JOB_SUBMISSION_FAILS
    }

    /** Test cleanup for a job that finishes ordinarily. */
    @Test
    public void testBlobServerCleanupFinishedJob() throws Exception {
        testBlobServerCleanup(TestCase.JOB_FINISHES_SUCESSFULLY);
    }

    /** Test cleanup for a job which is cancelled after submission. */
    @Test
    public void testBlobServerCleanupCancelledJob() throws Exception {
        testBlobServerCleanup(TestCase.JOB_IS_CANCELLED);
    }

    /**
     * Test cleanup for a job that fails (first a task fails, then the job recovers, then the whole
     * job fails due to a limited restart policy).
     */
    @Test
    public void testBlobServerCleanupFailedJob() throws Exception {
        testBlobServerCleanup(TestCase.JOB_FAILS);
    }

    /**
     * Test cleanup for a job that fails job submission (emulated by an additional BLOB not being
     * present).
     */
    @Test
    public void testBlobServerCleanupFailedSubmission() throws Exception {
        testBlobServerCleanup(TestCase.JOB_SUBMISSION_FAILS);
    }

    private void testBlobServerCleanup(final TestCase testCase) throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final int numTasks = 2;
        final Deadline timeout = Deadline.fromNow(Duration.ofSeconds(30L));

        final JobGraph jobGraph = createJobGraph(testCase, numTasks);
        final JobID jid = jobGraph.getJobID();

        // upload a blob
        final File tempBlob = File.createTempFile("Required", ".jar");
        final int blobPort = miniCluster.getClusterInformation().getBlobServerPort();
        List<PermanentBlobKey> keys =
                BlobClient.uploadFiles(
                        new InetSocketAddress("localhost", blobPort),
                        configuration,
                        jid,
                        Collections.singletonList(new Path(tempBlob.getAbsolutePath())));
        assertThat(keys, hasSize(1));
        jobGraph.addUserJarBlobKey(keys.get(0));

        if (testCase == TestCase.JOB_SUBMISSION_FAILS) {
            // add an invalid key so that the submission fails
            jobGraph.addUserJarBlobKey(new PermanentBlobKey());
        }

        final JobSubmissionResult jobSubmissionResult = miniCluster.submitJob(jobGraph).get();

        if (testCase == TestCase.JOB_SUBMISSION_FAILS) {
            // Wait for submission to fail & check if exception is forwarded
            Optional<SerializedThrowable> exception =
                    miniCluster.requestJobResult(jid).get().getSerializedThrowable();
            assertTrue(exception.isPresent());
            assertTrue(
                    ExceptionUtils.findThrowableSerializedAware(
                                    exception.get(),
                                    NoSuchFileException.class,
                                    getClass().getClassLoader())
                            .isPresent());

            // check job status
            assertThat(miniCluster.getJobStatus(jid).get(), is(JobStatus.FAILED));
        } else {
            assertThat(jobSubmissionResult.getJobID(), is(jid));

            final CompletableFuture<JobResult> resultFuture = miniCluster.requestJobResult(jid);

            if (testCase == TestCase.JOB_FAILS) {
                // fail a task so that the job is going to be recovered (we actually do not
                // need the blocking part of the invokable and can start throwing right away)
                FailingBlockingInvokable.unblock();

                // job will get restarted, BlobCache may re-download the BLOB if already deleted
                // then the tasks will fail again and the restart strategy will finalise the job
                final JobResult jobResult = resultFuture.get();
                assertThat(jobResult.isSuccess(), is(false));
                assertThat(jobResult.getApplicationStatus(), is(ApplicationStatus.FAILED));
            } else if (testCase == TestCase.JOB_IS_CANCELLED) {

                miniCluster.cancelJob(jid);

                final JobResult jobResult = resultFuture.get();
                assertThat(jobResult.isSuccess(), is(false));
                assertThat(jobResult.getApplicationStatus(), is(ApplicationStatus.CANCELED));
            } else {
                final JobResult jobResult = resultFuture.get();
                Throwable cause =
                        jobResult
                                .getSerializedThrowable()
                                .map(
                                        throwable ->
                                                throwable.deserializeError(
                                                        getClass().getClassLoader()))
                                .orElse(null);
                assertThat(
                        ExceptionUtils.stringifyException(cause), jobResult.isSuccess(), is(true));
            }
        }

        // both BlobServer and BlobCache should eventually delete all files

        File[] blobDirs = blobBaseDir.listFiles((dir, name) -> name.startsWith("blobStore-"));
        assertNotNull(blobDirs);
        for (File blobDir : blobDirs) {
            waitForEmptyBlobDir(blobDir, timeout.timeLeft());
        }
    }

    @Nonnull
    private JobGraph createJobGraph(TestCase testCase, int numTasks) {
        JobVertex source = new JobVertex("Source");
        if (testCase == TestCase.JOB_FAILS) {
            source.setInvokableClass(FailingBlockingInvokable.class);
        } else if (testCase == TestCase.JOB_IS_CANCELLED) {
            source.setInvokableClass(BlockingNoOpInvokable.class);
        } else {
            source.setInvokableClass(NoOpInvokable.class);
        }
        source.setParallelism(numTasks);

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .setJobId(new JobID(0, testCase.ordinal()))
                .addJobVertex(source)
                .build();
    }

    /**
     * Waits until the given {@link org.apache.flink.runtime.blob.BlobService} storage directory
     * does not contain any job-related folders any more.
     *
     * @param blobDir directory of a {@link org.apache.flink.runtime.blob.BlobServer} or {@link
     *     org.apache.flink.runtime.blob.BlobCacheService}
     * @param remaining remaining time for this test
     * @see org.apache.flink.runtime.blob.BlobUtils
     */
    private static void waitForEmptyBlobDir(File blobDir, Duration remaining)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + remaining.toMillis();
        String[] blobDirContents;
        final FilenameFilter jobDirFilter = (dir, name) -> name.startsWith("job_");

        do {
            blobDirContents = blobDir.list(jobDirFilter);
            if (blobDirContents == null || blobDirContents.length == 0) {
                return;
            }
            Thread.sleep(RETRY_INTERVAL);
        } while (System.currentTimeMillis() < deadline);

        fail(
                "Timeout while waiting for "
                        + blobDir.getAbsolutePath()
                        + " to become empty. Current contents: "
                        + Arrays.toString(blobDirContents));
    }
}
