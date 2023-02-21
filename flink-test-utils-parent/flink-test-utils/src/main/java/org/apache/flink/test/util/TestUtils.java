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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ExceptionUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;
import static org.junit.Assert.fail;

/** Test utilities. */
public class TestUtils {

    /**
     * Execute the job and wait for the job result synchronously.
     *
     * @throws Exception If executing the environment throws an exception which does not have {@link
     *     SuccessException} as a cause.
     */
    public static void tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
        JobClient jobClient = null;
        try {
            StreamGraph graph = see.getStreamGraph();
            graph.setJobName(name);
            jobClient = see.executeAsync(graph);
            jobClient.getJobExecutionResult().get();
        } catch (Throwable root) {
            if (jobClient != null) {
                try {
                    jobClient.cancel().get();
                } catch (Exception e) {
                    // Exception could be thrown if the job has already finished.
                    // Ignore the exception.
                }
            }

            Optional<SuccessException> successAsCause =
                    ExceptionUtils.findThrowable(root, SuccessException.class);

            if (!successAsCause.isPresent()) {
                root.printStackTrace();
                fail("Test failed: " + root.getMessage());
            }
        }
    }

    public static void submitJobAndWaitForResult(
            ClusterClient<?> client, JobGraph jobGraph, ClassLoader classLoader) throws Exception {
        client.submitJob(jobGraph)
                .thenCompose(client::requestJobResult)
                .get()
                .toJobExecutionResult(classLoader);
    }

    public static CheckpointMetadata loadCheckpointMetadata(String savepointPath)
            throws IOException {
        CompletedCheckpointStorageLocation location =
                AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(savepointPath);

        try (DataInputStream stream =
                new DataInputStream(location.getMetadataHandle().openInputStream())) {
            return Checkpoints.loadCheckpointMetadata(
                    stream, Thread.currentThread().getContextClassLoader(), savepointPath);
        }
    }

    /**
     * @deprecated please use {@link
     *     org.apache.flink.runtime.testutils.CommonTestUtils#getLatestCompletedCheckpointPath(JobID,
     *     MiniCluster)} which is less prone to {@link NoSuchFileException} and IO-intensive.
     */
    @Deprecated
    public static File getMostRecentCompletedCheckpoint(File checkpointDir) throws IOException {
        return getMostRecentCompletedCheckpointMaybe(checkpointDir)
                .orElseThrow(() -> new IllegalStateException("Cannot generate checkpoint"));
    }

    /**
     * @deprecated please use {@link
     *     org.apache.flink.runtime.testutils.CommonTestUtils#getLatestCompletedCheckpointPath(JobID,
     *     MiniCluster)} which is less prone to {@link NoSuchFileException} and IO-intensive.
     */
    @Deprecated
    public static Optional<File> getMostRecentCompletedCheckpointMaybe(File checkpointDir)
            throws IOException {
        return Files.find(checkpointDir.toPath(), 2, TestUtils::isCompletedCheckpoint)
                .max(Comparator.comparing(Path::toString))
                .map(Path::toFile);
    }

    private static boolean isCompletedCheckpoint(Path path, BasicFileAttributes attr) {
        return attr.isDirectory()
                && path.getFileName().toString().startsWith(CHECKPOINT_DIR_PREFIX)
                && hasMetadata(path);
    }

    private static boolean hasMetadata(Path file) {
        try {
            return Files.find(
                            file.toAbsolutePath(),
                            1,
                            (path, attrs) ->
                                    path.getFileName().toString().equals(METADATA_FILE_NAME))
                    .findAny()
                    .isPresent();
        } catch (UncheckedIOException uncheckedIOException) {
            // return false when the metadata file is in progress due to subsumed checkpoint
            if (ExceptionUtils.findThrowable(uncheckedIOException, NoSuchFileException.class)
                    .isPresent()) {
                return false;
            }
            throw uncheckedIOException;
        } catch (IOException ioException) {
            ExceptionUtils.rethrow(ioException);
            return false; // should never happen
        }
    }

    /**
     * @deprecated please use {@link
     *     org.apache.flink.runtime.testutils.CommonTestUtils#waitForCheckpoint(JobID, MiniCluster,
     *     Deadline)} which is less prone to {@link NoSuchFileException} and IO-intensive.
     */
    @Deprecated
    public static void waitUntilExternalizedCheckpointCreated(File checkpointDir)
            throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(50);
            Optional<File> externalizedCheckpoint =
                    getMostRecentCompletedCheckpointMaybe(checkpointDir);
            if (externalizedCheckpoint.isPresent()) {
                break;
            }
        }
    }

    public static void waitUntilJobCanceled(JobID jobId, ClusterClient<?> client)
            throws ExecutionException, InterruptedException {
        while (client.getJobStatus(jobId).get() != JobStatus.CANCELED) {
            Thread.sleep(50);
        }
    }

    /**
     * Wait util all task of a job turns into RUNNING state.
     *
     * @param restClusterClient RestClusterClient which could be {@link
     *     org.apache.flink.test.junit5.InjectClusterClient}.
     */
    public static void waitUntilAllTasksAreRunning(
            RestClusterClient<?> restClusterClient, JobID jobId) throws Exception {
        // access the REST endpoint of the cluster to determine the state of each ExecutionVertex
        final JobDetailsHeaders detailsHeaders = JobDetailsHeaders.getInstance();
        final JobMessageParameters params = detailsHeaders.getUnresolvedMessageParameters();
        params.jobPathParameter.resolve(jobId);

        CommonTestUtils.waitUntilCondition(
                () ->
                        restClusterClient
                                .sendRequest(detailsHeaders, params, EmptyRequestBody.getInstance())
                                .thenApply(
                                        detailsInfo ->
                                                allVerticesRunning(
                                                        detailsInfo.getJobVerticesPerState()))
                                .get());
    }

    private static boolean allVerticesRunning(Map<ExecutionState, Integer> states) {
        return states.entrySet().stream()
                .allMatch(
                        entry -> {
                            if (entry.getKey() == ExecutionState.RUNNING) {
                                return entry.getValue() > 0;
                            } else {
                                return entry.getValue() == 0; // no vertices in non-running state.
                            }
                        });
    }
}
