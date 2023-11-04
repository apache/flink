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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.jupiter.api.Assertions.fail;

/** This class contains auxiliary methods for unit tests. */
public class CommonTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonTestUtils.class);

    private static final long RETRY_INTERVAL = 100L;

    /**
     * Gets the classpath with which the current JVM was started.
     *
     * @return The classpath with which the current JVM was started.
     */
    public static String getCurrentClasspath() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        return bean.getClassPath();
    }

    /** Create a temporary log4j configuration for the test. */
    public static File createTemporaryLog4JProperties() throws IOException {
        File log4jProps = File.createTempFile(FileUtils.getRandomFilename(""), "-log4j.properties");
        log4jProps.deleteOnExit();
        CommonTestUtils.printLog4jDebugConfig(log4jProps);

        return log4jProps;
    }

    /**
     * Tries to get the java executable command with which the current JVM was started. Returns
     * null, if the command could not be found.
     *
     * @return The java executable command.
     */
    public static String getJavaCommandPath() {
        File javaHome = new File(System.getProperty("java.home"));

        String path1 = new File(javaHome, "java").getAbsolutePath();
        String path2 = new File(new File(javaHome, "bin"), "java").getAbsolutePath();

        try {
            ProcessBuilder bld = new ProcessBuilder(path1, "-version");
            Process process = bld.start();
            if (process.waitFor() == 0) {
                return path1;
            }
        } catch (Throwable t) {
            // ignore and try the second path
        }

        try {
            ProcessBuilder bld = new ProcessBuilder(path2, "-version");
            Process process = bld.start();
            if (process.waitFor() == 0) {
                return path2;
            }
        } catch (Throwable tt) {
            // no luck
        }
        return null;
    }

    public static void printLog4jDebugConfig(File file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("rootLogger.level = DEBUG");
            writer.println("rootLogger.appenderRef.console.ref = ConsoleAppender");
            writer.println("appender.console.name = ConsoleAppender");
            writer.println("appender.console.type = CONSOLE");
            writer.println("appender.console.target = SYSTEM_ERR");
            writer.println("appender.console.layout.type = PatternLayout");
            writer.println(
                    "appender.console.layout.pattern = %d{HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n");
            writer.println("logger.jetty.name = org.eclipse.jetty.util.log");
            writer.println("logger.jetty.level = OFF");
            writer.println("logger.zookeeper.name = org.apache.zookeeper");
            writer.println("logger.zookeeper.level = OFF");
            writer.flush();
        }
    }

    public static void waitUntilCondition(SupplierWithException<Boolean, Exception> condition)
            throws Exception {
        waitUntilCondition(condition, RETRY_INTERVAL);
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition, long retryIntervalMillis)
            throws Exception {
        while (!condition.get()) {
            Thread.sleep(retryIntervalMillis);
        }
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition, int retryAttempts)
            throws Exception {
        waitUntilCondition(condition, RETRY_INTERVAL, retryAttempts);
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            long retryIntervalMillis,
            int retryAttempts)
            throws Exception {
        while (!condition.get() && retryAttempts != 0) {
            retryAttempts--;
            LOG.debug("Condition not true. Remaining retry attempts {}", retryAttempts);
            LOG.debug("Sleeping for {} milliseconds", retryIntervalMillis);
            Thread.sleep(retryIntervalMillis);
        }
        if (retryAttempts == 0) {
            throw new FlinkException("Exhausted retry attempts.");
        } else {
            LOG.debug("Condition true. Remaining retry attempts {}", retryAttempts);
        }
    }

    public static void waitForAllTaskRunning(
            MiniCluster miniCluster, JobID jobId, boolean allowFinished) throws Exception {
        waitForAllTaskRunning(() -> getGraph(miniCluster, jobId), allowFinished);
    }

    private static AccessExecutionGraph getGraph(MiniCluster miniCluster, JobID jobId)
            throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        return miniCluster.getExecutionGraph(jobId).get(60, TimeUnit.SECONDS);
    }

    public static void waitForAllTaskRunning(
            SupplierWithException<AccessExecutionGraph, Exception> executionGraphSupplier,
            boolean allowFinished)
            throws Exception {
        Predicate<AccessExecutionVertex> subtaskPredicate =
                task -> {
                    switch (task.getExecutionState()) {
                        case RUNNING:
                            return true;
                        case FINISHED:
                            if (allowFinished) {
                                return true;
                            } else {
                                throw new RuntimeException("Sub-Task finished unexpectedly" + task);
                            }
                        default:
                            return false;
                    }
                };
        waitUntilCondition(
                () -> {
                    final AccessExecutionGraph graph = executionGraphSupplier.get();
                    if (graph.getState().isGloballyTerminalState()) {
                        final ErrorInfo failureInfo = graph.getFailureInfo();
                        fail(
                                format(
                                        "Graph is in globally terminal state (%s)",
                                        graph.getState()),
                                failureInfo != null ? failureInfo.getException() : null);
                    }
                    return graph.getState() == JobStatus.RUNNING
                            && graph.getAllVertices().values().stream()
                                    .allMatch(
                                            jobVertex ->
                                                    Arrays.stream(jobVertex.getTaskVertices())
                                                            .allMatch(subtaskPredicate));
                });
    }

    public static void waitForAllTaskRunning(
            SupplierWithException<JobDetailsInfo, Exception> jobDetailsSupplier) throws Exception {
        waitUntilCondition(
                () -> {
                    final JobDetailsInfo jobDetailsInfo = jobDetailsSupplier.get();
                    final Collection<JobDetailsInfo.JobVertexDetailsInfo> vertexInfos =
                            jobDetailsInfo.getJobVertexInfos();
                    if (vertexInfos.size() == 0) {
                        return false;
                    }
                    for (JobDetailsInfo.JobVertexDetailsInfo vertexInfo : vertexInfos) {
                        final Integer numRunningTasks =
                                vertexInfo.getTasksPerState().get(ExecutionState.RUNNING);
                        if (numRunningTasks == null
                                || numRunningTasks != vertexInfo.getParallelism()) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    public static void waitForNoTaskRunning(
            SupplierWithException<JobDetailsInfo, Exception> jobDetailsSupplier) throws Exception {
        waitUntilCondition(
                () -> {
                    final Map<ExecutionState, Integer> state =
                            jobDetailsSupplier.get().getJobVerticesPerState();
                    final Integer numRunningTasks = state.get(ExecutionState.RUNNING);
                    return numRunningTasks == null || numRunningTasks.equals(0);
                });
    }

    public static void waitUntilJobManagerIsInitialized(
            SupplierWithException<JobStatus, Exception> jobStatusSupplier) throws Exception {
        waitUntilCondition(() -> jobStatusSupplier.get() != JobStatus.INITIALIZING, 20L);
    }

    public static void waitForJobStatus(JobClient client, List<JobStatus> expectedStatus)
            throws Exception {
        waitUntilCondition(
                () -> {
                    final JobStatus currentStatus = client.getJobStatus().get();

                    // Entered an expected status
                    if (expectedStatus.contains(currentStatus)) {
                        return true;
                    }

                    // Entered a terminal status but not expected
                    if (currentStatus.isTerminalState()) {
                        try {
                            // Exception will be exposed here if job failed
                            client.getJobExecutionResult().get();
                        } catch (Exception e) {
                            throw new IllegalStateException(
                                    format(
                                            "Job has entered %s state, but expecting %s",
                                            currentStatus, expectedStatus),
                                    e);
                        }
                        throw new IllegalStateException(
                                format(
                                        "Job has entered a terminal state %s, but expecting %s",
                                        currentStatus, expectedStatus));
                    }

                    // Continue waiting for expected status
                    return false;
                });
    }

    public static void terminateJob(JobClient client) throws Exception {
        client.cancel().get();
    }

    public static void waitForSubtasksToFinish(
            MiniCluster miniCluster, JobID job, JobVertexID id, boolean allSubtasks)
            throws Exception {
        Predicate<AccessExecutionVertex> subtaskPredicate =
                subtask -> {
                    ExecutionState state = subtask.getExecutionState();
                    if (state == ExecutionState.FINISHED) {
                        return true;
                    } else if (state.isTerminal()) {
                        throw new RuntimeException(
                                format(
                                        "Sub-Task %s is already in a terminal state %s",
                                        subtask, state));
                    } else {
                        return false;
                    }
                };
        waitUntilCondition(
                () -> {
                    AccessExecutionGraph graph = getGraph(miniCluster, job);
                    if (graph.getState() != JobStatus.RUNNING) {
                        return false;
                    }
                    Stream<AccessExecutionVertex> vertexStream =
                            Arrays.stream(
                                    graph.getAllVertices().values().stream()
                                            .filter(jv -> jv.getJobVertexId().equals(id))
                                            .findAny()
                                            .orElseThrow(
                                                    () ->
                                                            new RuntimeException(
                                                                    "Vertex not found " + id))
                                            .getTaskVertices());
                    return allSubtasks
                            ? vertexStream.allMatch(subtaskPredicate)
                            : vertexStream.anyMatch(subtaskPredicate);
                });
    }

    /** Wait for (at least) the given number of successful checkpoints. */
    public static void waitForCheckpoint(JobID jobID, MiniCluster miniCluster, int numCheckpoints)
            throws Exception, FlinkJobNotFoundException {
        waitUntilCondition(
                () -> {
                    AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobID).get();
                    if (Optional.ofNullable(graph.getCheckpointStatsSnapshot())
                            .filter(
                                    st ->
                                            st.getCounts().getNumberOfCompletedCheckpoints()
                                                    >= numCheckpoints)
                            .isPresent()) {
                        return true;
                    } else if (graph.getState().isGloballyTerminalState()) {
                        checkState(
                                graph.getFailureInfo() != null,
                                "Job terminated before taking required %s checkpoints: %s",
                                numCheckpoints,
                                graph.getState());
                        throw graph.getFailureInfo().getException();
                    } else {
                        return false;
                    }
                });
    }

    /** Wait for on more completed checkpoint. */
    public static void waitForOneMoreCheckpoint(JobID jobID, MiniCluster miniCluster)
            throws Exception {
        final long[] currentCheckpoint = new long[] {-1L};
        waitUntilCondition(
                () -> {
                    AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobID).get();
                    CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
                    if (snapshot != null) {
                        long currentCount = snapshot.getCounts().getNumberOfCompletedCheckpoints();
                        if (currentCheckpoint[0] < 0L) {
                            currentCheckpoint[0] = currentCount;
                        } else {
                            return currentCount > currentCheckpoint[0];
                        }
                    } else if (graph.getState().isGloballyTerminalState()) {
                        checkState(
                                graph.getFailureInfo() != null,
                                "Job terminated before taking required checkpoint.",
                                graph.getState());
                        throw graph.getFailureInfo().getException();
                    }
                    return false;
                });
    }

    /**
     * @return the path as {@link java.net.URI} to the latest checkpoint.
     * @throws FlinkJobNotFoundException if job not found
     */
    public static Optional<String> getLatestCompletedCheckpointPath(
            JobID jobID, MiniCluster cluster)
            throws InterruptedException, ExecutionException, FlinkJobNotFoundException {
        return Optional.ofNullable(
                        cluster.getExecutionGraph(jobID).get().getCheckpointStatsSnapshot())
                .flatMap(
                        stats ->
                                Optional.ofNullable(
                                        stats.getHistory().getLatestCompletedCheckpoint()))
                .map(CompletedCheckpointStats::getExternalPath);
    }

    /** Utility class to read the output of a process stream and forward it into a StringWriter. */
    public static class PipeForwarder extends Thread {

        private final StringWriter target;
        private final InputStream source;

        public PipeForwarder(InputStream source, StringWriter target) {
            super("Pipe Forwarder");
            setDaemon(true);

            this.source = source;
            this.target = target;

            start();
        }

        @Override
        public void run() {
            try {
                int next;
                while ((next = source.read()) != -1) {
                    target.write(next);
                }
            } catch (IOException e) {
                // terminate
            }
        }
    }

    public static boolean isStreamContentEqual(InputStream input1, InputStream input2)
            throws IOException {

        if (!(input1 instanceof BufferedInputStream)) {
            input1 = new BufferedInputStream(input1);
        }
        if (!(input2 instanceof BufferedInputStream)) {
            input2 = new BufferedInputStream(input2);
        }

        int ch = input1.read();
        while (-1 != ch) {
            int ch2 = input2.read();
            if (ch != ch2) {
                return false;
            }
            ch = input1.read();
        }

        int ch2 = input2.read();
        return (ch2 == -1);
    }
}
