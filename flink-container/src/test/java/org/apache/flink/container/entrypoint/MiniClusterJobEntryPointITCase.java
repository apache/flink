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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.externalresource.ExternalResourceDriver;
import org.apache.flink.api.common.externalresource.ExternalResourceDriverFactory;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricherFactory;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.highavailability.ApplicationResultStoreOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorageFactory;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Runs {@link MiniClusterJobEntryPoint}, configured the way that Flink Kubernetes Operator
 * configures an application deployment: ZooKeeper HA, a fixed JobID in the config file, the cluster
 * kept up after the application finishes, and result stores retained on commit.
 */
class MiniClusterJobEntryPointITCase {

    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    /** How long a restarted process has to stay up to count as having survived the restart. */
    private static final Duration STAYS_UP_WINDOW = Duration.ofSeconds(5);

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @TempDir private Path tempDir;

    private Path confDir;
    private Path markerFile;
    private JobID jobId;
    private Path log4jConfig;
    private int restPort;
    private String haClusterId;

    @BeforeEach
    void writeOperatorStyleConfiguration() throws Exception {
        confDir = Files.createDirectories(tempDir.resolve("conf"));
        markerFile = tempDir.resolve("main-invocations");
        jobId = JobID.generate();
        try (NetUtils.Port port = NetUtils.getAvailablePort()) {
            restPort = port.getPort();
        }

        log4jConfig = tempDir.resolve("log4j2.properties");
        Files.write(
                log4jConfig,
                List.of(
                        "rootLogger.level = INFO",
                        "rootLogger.appenderRef.console.ref = ConsoleAppender",
                        "appender.console.name = ConsoleAppender",
                        "appender.console.type = CONSOLE",
                        "appender.console.layout.type = PatternLayout",
                        "appender.console.layout.pattern = %d{HH:mm:ss,SSS} %-5p %c{1} - %m%n"));

        haClusterId = "minicluster-itcase-" + UUID.randomUUID();
        final String haStorage = tempDir.resolve("ha").toUri().toString();
        Files.write(
                confDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
                List.of(
                        entry(HighAvailabilityOptions.HA_MODE, "zookeeper"),
                        entry(
                                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                                ZOOKEEPER.getCustomExtension().getConnectString()),
                        entry(HighAvailabilityOptions.HA_STORAGE_PATH, haStorage),
                        entry(HighAvailabilityOptions.HA_CLUSTER_ID, haClusterId),
                        entry(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString()),
                        entry(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false),
                        entry(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true),
                        entry(JobResultStoreOptions.DELETE_ON_COMMIT, false),
                        entry(ApplicationResultStoreOptions.DELETE_ON_COMMIT, false),
                        entry(RestOptions.ADDRESS, "localhost"),
                        entry(RestOptions.PORT, restPort),
                        entry(RestOptions.BIND_PORT, restPort)));
    }

    @Test
    void restartAfterApplicationFinishedDoesNotRerunMain() throws Exception {
        final Process first = startMiniClusterProcess("first");
        try {
            awaitJobState(first, "first", "FINISHED");
            assertThat(first.isAlive())
                    .as("the process stays up after the application finishes")
                    .isTrue();
        } finally {
            // SIGTERM is what Kubernetes sends when pods are deleted
            stop(first);
        }
        assertThat(mainInvocations()).isEqualTo(1);
        assertThat(zooKeeperNodeExists("/flink/" + haClusterId))
                .as("stopping the process keeps the HA metadata, although the application finished")
                .isTrue();

        final Process second = startMiniClusterProcess("second");
        try {
            awaitClusterServing(second, "second");
            assertStaysUp(second, "second");
            assertThat(mainInvocations())
                    .as("the restarted process must not run main() again")
                    .isEqualTo(1);
        } finally {
            stop(second);
        }
    }

    @Test
    void applicationErrorIsReportedAsFailedJobWhileProcessStaysUp() throws Exception {
        final List<String> arguments = applicationArguments(FailingJob.class, List.of());

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        try {
            awaitJobState(first, "first", "FAILED");
            assertStaysUp(first, "first");
        } finally {
            stop(first);
        }
        assertThat(mainInvocations()).isEqualTo(1);

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            awaitClusterServing(second, "second");
            assertStaysUp(second, "second");
            assertThat(mainInvocations())
                    .as("the restarted process must not run main() again")
                    .isEqualTo(1);
        } finally {
            stop(second);
        }
    }

    @Test
    void standaloneApplicationClusterRestartAfterApplicationFinishes() throws Exception {
        final Configuration taskManagerMemory = new Configuration();
        taskManagerMemory.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1536m"));

        final String dynamicConfigs =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(
                        TaskExecutorProcessUtils.processSpecFromConfig(taskManagerMemory));

        final List<String> taskManagerArguments = new ArrayList<>();
        Collections.addAll(
                taskManagerArguments,
                "--configDir",
                confDir.toString(),
                "-D" + TaskManagerOptions.HOST.key() + "=localhost");
        Collections.addAll(taskManagerArguments, dynamicConfigs.trim().split("\\s+"));

        final Process taskManager =
                startProcess("taskmanager", TaskManagerRunner.class, taskManagerArguments);
        try {
            final Process first = startJobManagerProcess("first");
            try {
                awaitJobState(first, "first", "FINISHED");
                assertThat(first.isAlive())
                        .as("the process stays up after the application finishes")
                        .isTrue();
            } finally {
                stop(first);
            }
            assertThat(mainInvocations()).isEqualTo(1);

            final Process second = startJobManagerProcess("second");
            try {
                awaitClusterServing(second, "second");
                assertStaysUp(second, "second");
                assertThat(mainInvocations())
                        .as("the restarted process must not run main() again")
                        .isEqualTo(1);
            } finally {
                stop(second);
            }
        } catch (AssertionError e) {
            printLog("taskmanager");
            throw e;
        } finally {
            stop(taskManager);
        }
    }

    private Process startJobManagerProcess(String name) throws IOException {
        final List<String> arguments = new ArrayList<>(applicationArguments());
        arguments.addAll(0, List.of("-D" + JobManagerOptions.ADDRESS.key() + "=localhost"));
        return startProcess(name, StandaloneApplicationClusterEntryPoint.class, arguments);
    }

    /** How the pod's previous process ended. */
    enum Termination {
        /** The pod was deleted or rescheduled: the process got to shut down. */
        SIGTERM,
        /** The process crashed, was OOM-killed, or its node was lost. */
        SIGKILL
    }

    @ParameterizedTest
    @EnumSource(Termination.class)
    void restartResumesJobFromLatestCheckpoint(Termination termination) throws Exception {
        final List<String> arguments = unboundedJobArguments();

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        final long lastCheckpointBeforeRestart;
        try {
            lastCheckpointBeforeRestart =
                    awaitRest(
                                    first,
                                    "first",
                                    checkpointsPath(),
                                    "three completed checkpoints",
                                    json -> json.at("/latest/completed/id").asLong() >= 3)
                            .at("/latest/completed/id")
                            .asLong();
        } finally {
            if (termination == Termination.SIGKILL) {
                first.destroyForcibly().waitFor();
            } else {
                stop(first);
            }
        }

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            final JsonNode restored =
                    awaitRest(
                                    second,
                                    "second",
                                    checkpointsPath(),
                                    "a restored checkpoint",
                                    json -> json.at("/latest/restored/id").isNumber())
                            .at("/latest/restored");
            assertThat(restored.get("is_savepoint").asBoolean()).isFalse();
            assertThat(restored.get("id").asLong())
                    .as("job resumes from latest checkpoint")
                    .isGreaterThanOrEqualTo(lastCheckpointBeforeRestart);

            awaitJobState(second, "second", "RUNNING");
            assertThat(second.isAlive()).isTrue();
            assertThat(mainInvocations())
                    .as("main() runs again when the restarted process recovers the job")
                    .isEqualTo(2);
        } finally {
            stop(second);
        }
    }

    @Test
    void restartAfterSigtermRecoversJobWithoutRestartStrategy() throws Exception {
        // A job failed over by its TaskManager stopping would fail globally without restarts,
        // which discards its high availability data. Only deployed tasks can be failed over, so
        // the test waits for them to run.
        final List<String> arguments =
                applicationArguments(
                        UnboundedJob.class,
                        List.of(dynamicProperty(RestartStrategyOptions.RESTART_STRATEGY, "none")));

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        try {
            awaitRest(
                    first,
                    "first",
                    "/jobs/" + jobId,
                    "the job's tasks to be running",
                    MiniClusterJobEntryPointITCase::allTasksRunning);
        } finally {
            stop(first);
        }

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            final Set<String> runningOrTerminal =
                    Set.of("RUNNING", "FAILED", "CANCELED", "FINISHED", "SUSPENDED");
            final String state =
                    awaitRest(
                                    second,
                                    "second",
                                    "/jobs/" + jobId,
                                    "the job to be running or terminal",
                                    json -> runningOrTerminal.contains(json.get("state").asText()))
                            .get("state")
                            .asText();
            assertThat(state).as("the restarted process recovers the job").isEqualTo("RUNNING");
        } finally {
            stop(second);
        }
    }

    @Test
    void restartLoopAfterSigkillResumesJobFromLatestCheckpoint() throws Exception {
        final List<String> arguments = unboundedJobArguments();

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        final long lastCheckpointBeforeRestart;
        try {
            lastCheckpointBeforeRestart =
                    awaitRest(
                                    first,
                                    "first",
                                    checkpointsPath(),
                                    "three completed checkpoints",
                                    json -> json.at("/latest/completed/id").asLong() >= 3)
                            .at("/latest/completed/id")
                            .asLong();
        } finally {
            first.destroyForcibly().waitFor();
        }

        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        final List<Integer> exitCodes = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            final String name = "attempt-" + (exitCodes.size() + 1);
            final Process attempt = startProcess(name, MiniClusterJobEntryPoint.class, arguments);
            try {
                final Optional<JsonNode> restored =
                        pollRest(
                                attempt,
                                checkpointsPath(),
                                json -> json.at("/latest/restored/id").isNumber(),
                                deadline);
                if (restored.isEmpty()) {
                    exitCodes.add(attempt.exitValue());
                    continue;
                }
                System.out.println(
                        "Resumed on attempt "
                                + (exitCodes.size() + 1)
                                + "; earlier attempts exited with "
                                + exitCodes);
                assertThat(restored.get().at("/latest/restored/id").asLong())
                        .as("the job resumes from latest checkpoint")
                        .isGreaterThanOrEqualTo(lastCheckpointBeforeRestart);
                awaitJobState(attempt, name, "RUNNING");
                return;
            } finally {
                stop(attempt);
            }
        }
        fail(
                "The job was not resumed within %s; %d attempts exited with %s.",
                TIMEOUT, exitCodes.size(), exitCodes);
    }

    @Test
    void restoresJobFromSavepointGivenOnCommandLine() throws Exception {
        final Process first =
                startProcess("first", MiniClusterJobEntryPoint.class, unboundedJobArguments());
        final String savepointPath;
        try {
            awaitRest(
                    first,
                    "first",
                    checkpointsPath(),
                    "a completed checkpoint",
                    json -> json.at("/latest/completed/id").asLong() >= 1);
            savepointPath = stopWithSavepoint(first, "first");
            awaitJobState(first, "first", "FINISHED");
        } finally {
            stop(first);
        }

        // a separate HA cluster, as for a job moved to a new deployment, so that the job can only
        // be restored from the savepoint and not recovered from HA metadata
        final List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "--fromSavepoint",
                                savepointPath,
                                dynamicProperty(
                                        HighAvailabilityOptions.HA_CLUSTER_ID,
                                        haClusterId + "-restored")));
        arguments.addAll(unboundedJobArguments());

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            final JsonNode restored =
                    awaitRest(
                                    second,
                                    "second",
                                    checkpointsPath(),
                                    "a restored savepoint",
                                    json -> json.at("/latest/restored/id").isNumber())
                            .at("/latest/restored");
            assertThat(restored.get("is_savepoint").asBoolean()).isTrue();
            assertThat(restored.get("external_path").asText()).isEqualTo(savepointPath);

            awaitJobState(second, "second", "RUNNING");
        } finally {
            stop(second);
        }
    }

    /** Stops the job with a savepoint, as the operator does for a savepoint upgrade. */
    private String stopWithSavepoint(Process process, String name) throws Exception {
        final JsonNode trigger =
                postRest(
                        "/jobs/" + jobId + "/stop",
                        OBJECT_MAPPER
                                .createObjectNode()
                                .put(
                                        "targetDirectory",
                                        tempDir.resolve("savepoints").toUri().toString())
                                .put("drain", false));
        final JsonNode operation =
                awaitRest(
                                process,
                                name,
                                "/jobs/"
                                        + jobId
                                        + "/savepoints/"
                                        + trigger.get("request-id").asText(),
                                "the savepoint",
                                json -> "COMPLETED".equals(json.at("/status/id").asText()))
                        .get("operation");
        assertThat(operation.has("failure-cause"))
                .as("the savepoint failed: %s", operation.get("failure-cause"))
                .isFalse();
        return operation.get("location").asText();
    }

    private List<String> unboundedJobArguments() {
        return applicationArguments(
                UnboundedJob.class,
                List.of(
                        dynamicProperty(CheckpointingOptions.CHECKPOINTING_INTERVAL, "500 ms"),
                        dynamicProperty(
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                                tempDir.resolve("checkpoints").toUri()),
                        dynamicProperty(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, "5 s")));
    }

    @Test
    void fatalErrorExitsProcessWithoutDiscardingHAData() throws Exception {
        final List<String> arguments = unboundedJobArguments();

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        try {
            awaitRest(
                    first,
                    "first",
                    checkpointsPath(),
                    "a completed checkpoint",
                    json -> json.at("/latest/completed/id").asLong() >= 1);
        } finally {
            first.destroyForcibly().waitFor();
        }

        // break the job's HA state, so that recovering it is a fatal error
        try (Stream<Path> files = Files.list(tempDir.resolve("ha").resolve(haClusterId))) {
            for (Path file : (Iterable<Path>) files::iterator) {
                if (file.getFileName().toString().startsWith("submittedExecutionPlan")) {
                    Files.delete(file);
                }
            }
        }

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            assertThat(awaitExit(second, "second"))
                    .as("the process exits so supervisor can restart")
                    .isEqualTo(2);
            assertThat(logFile("second"))
                    .content()
                    .contains(
                            "ERROR ApplicationModeMiniCluster - JobManager failed. Shutting the"
                                    + " MiniCluster down.");
            assertThat(zooKeeperNodeExists(executionPlanZooKeeperPath()))
                    .as("the job's HA metadata survives the error")
                    .isTrue();
        } finally {
            stop(second);
        }
    }

    @Test
    void taskManagerFatalErrorExitsProcessWithoutDiscardingHAData() throws Exception {
        final List<String> arguments =
                applicationArguments(
                        StuckOnCancelJob.class,
                        List.of(
                                dynamicProperty(
                                        CheckpointingOptions.CHECKPOINTING_INTERVAL, "500 ms"),
                                dynamicProperty(
                                        TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, "2 s"),
                                dynamicProperty(
                                        TaskManagerOptions.TASK_CANCELLATION_INTERVAL, "500 ms")));

        final Process process = startProcess("process", MiniClusterJobEntryPoint.class, arguments);
        try {
            assertThat(awaitExit(process, "process"))
                    .as("the process exits so supervisor can restart")
                    .isEqualTo(2);
            assertThat(logFile("process"))
                    .content()
                    .contains(
                            "ERROR ApplicationModeMiniCluster - TaskManager #0 failed. Shutting the"
                                    + " MiniCluster down.");
            assertThat(zooKeeperNodeExists(executionPlanZooKeeperPath()))
                    .as("the job's HA metadata survives the error")
                    .isTrue();
        } finally {
            stop(process);
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {RecordingJob.class, FailingJob.class})
    void shutdownOnApplicationFinishExitsWithApplicationsExitCode(Class<?> jobClass)
            throws Exception {
        final Path webTmpDir = Files.createDirectories(tempDir.resolve("web"));
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                jobClass,
                                List.of(
                                        dynamicProperty(
                                                DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH,
                                                true),
                                        dynamicProperty(WebOptions.TMP_DIR, webTmpDir))));
        try {
            final ApplicationStatus expected =
                    jobClass == RecordingJob.class
                            ? ApplicationStatus.SUCCEEDED
                            : ApplicationStatus.FAILED;
            assertThat(awaitExit(process, "process"))
                    .as("the same exit code a standalone application cluster uses for %s", expected)
                    // as the operating system reports it: only the low eight bits survive
                    .isEqualTo(expected.processExitCode() & 0xFF);
            assertThat(zooKeeperNodeExists("/flink/" + haClusterId))
                    .as("the HA metadata is removed, as by a standalone application cluster")
                    .isFalse();
            assertThat(webTmpDir).as("the process's web directory is removed").isEmptyDirectory();
        } finally {
            stop(process);
        }
    }

    @Test
    void failedShutdownAfterApplicationFinishesExitsWithRuntimeFailureCode() throws Exception {
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(
                                        dynamicProperty(
                                                DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH,
                                                true),
                                        dynamicProperty(
                                                HighAvailabilityOptions.HA_MODE,
                                                FailingCleanupHaServicesFactory.class.getName()))));
        try {
            assertThat(awaitExit(process, "process"))
                    .as(
                            "the exit code a standalone application cluster uses if shutting down fails")
                    .isEqualTo(2);
            assertThat(logFile("process"))
                    .content()
                    .contains("ERROR MiniClusterJobEntryPoint - Could not close the MiniCluster.")
                    .contains(FailingCleanupHaServicesFactory.FAILURE_MESSAGE)
                    .contains("Application finished with status SUCCEEDED.");
        } finally {
            stop(process);
        }
    }

    @Test
    void sigtermDuringShutdownWaitsForTheShutdown() throws Exception {
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(
                                        dynamicProperty(
                                                DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH,
                                                true),
                                        dynamicProperty(
                                                HighAvailabilityOptions.HA_MODE,
                                                SlowCleanupHaServicesFactory.class.getName()),
                                        dynamicProperty(
                                                SlowCleanupHaServicesFactory.MARKER_DIR,
                                                tempDir))));
        try {
            awaitFile(process, "process", tempDir.resolve(SlowCleanupHaServicesFactory.STARTED));
            process.destroy();
            awaitExit(process, "process");

            assertThat(tempDir.resolve(SlowCleanupHaServicesFactory.FINISHED))
                    .as("the HA cleanup that was under way when SIGTERM arrived finishes")
                    .exists();
        } finally {
            stop(process);
        }
    }

    @Test
    void usesItsOwnWebDirectoryAndRemovesItOnShutdown() throws Exception {
        final Path webTmpDir = Files.createDirectories(tempDir.resolve("web"));
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(dynamicProperty(WebOptions.TMP_DIR, webTmpDir))));
        try {
            awaitClusterServing(process, "process");
            try (Stream<Path> webDirs = Files.list(webTmpDir)) {
                assertThat(webDirs)
                        .as("a directory of its own, as a cluster entrypoint uses")
                        .singleElement()
                        .satisfies(
                                dir ->
                                        assertThat(dir.getFileName().toString())
                                                .matches("flink-web-[0-9a-f-]{36}"));
            }

            stop(process);

            assertThat(webTmpDir).isEmptyDirectory();
        } finally {
            stop(process);
        }
    }

    @Test
    void runsJobFromJarsGivenAsLocalUris() throws Exception {
        final Path jar = tempDir.resolve("job.jar");
        writeJar(jar, RecordingJob.class);

        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        List.of(
                                "--configDir",
                                confDir.toString(),
                                "--jars",
                                "local://" + jar.toAbsolutePath(),
                                "--",
                                markerFile.toString()));
        try {
            awaitJobState(process, "process", "FINISHED");
            assertThat(mainInvocations()).isEqualTo(1);
        } finally {
            stop(process);
        }
    }

    @Test
    void restartAfterApplicationFinishedWithShutdownOnFinishExitsWithoutRerunningMain()
            throws Exception {
        final List<String> arguments =
                applicationArguments(
                        RecordingJob.class,
                        List.of(
                                dynamicProperty(
                                        DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, true)));

        final Process first = startProcess("first", MiniClusterJobEntryPoint.class, arguments);
        try {
            assertThat(awaitExit(first, "first")).isZero();
        } finally {
            stop(first);
        }

        final Process second = startProcess("second", MiniClusterJobEntryPoint.class, arguments);
        try {
            assertThat(awaitExit(second, "second")).isZero();
            assertThat(mainInvocations())
                    .as("the restarted process must not run main() again")
                    .isEqualTo(1);
        } finally {
            stop(second);
        }
    }

    @Test
    void startsJmxServerOnConfiguredPort() throws Exception {
        final int jmxPort;
        try (NetUtils.Port port = NetUtils.getAvailablePort()) {
            jmxPort = port.getPort();
        }
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(
                                        dynamicProperty(
                                                JMXServerOptions.JMX_SERVER_PORT, jmxPort))));
        try {
            awaitClusterServing(process, "process");
            final JMXServiceURL url =
                    new JMXServiceURL(
                            "service:jmx:rmi://localhost:"
                                    + jmxPort
                                    + "/jndi/rmi://localhost:"
                                    + jmxPort
                                    + "/jmxrmi");
            try (JMXConnector connector = JMXConnectorFactory.connect(url)) {
                assertThat(connector.getMBeanServerConnection().getMBeanCount()).isPositive();
            }
        } finally {
            stop(process);
        }
    }

    @Test
    void rpcServiceUsesConfiguredThreadPool() throws Exception {
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(
                                        dynamicProperty(
                                                RpcOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN, 16),
                                        dynamicProperty(
                                                RpcOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX,
                                                16))));
        try {
            // threads are only started on demand, so this waits for the cluster to need more than
            // the at most 4 threads of the MiniCluster's default pool
            final long deadline = System.nanoTime() + TIMEOUT.toNanos();
            long rpcThreads = 0;
            while (rpcThreads <= 4) {
                if (!process.isAlive() || System.nanoTime() > deadline) {
                    printLog("process");
                    fail(
                            "Expected more than 4 RPC threads within %s (last seen: %d).",
                            TIMEOUT, rpcThreads);
                }
                Thread.sleep(POLL_INTERVAL.toMillis());
                rpcThreads =
                        queryRest("/jobmanager/thread-dump")
                                .map(MiniClusterJobEntryPointITCase::rpcThreads)
                                .orElse(0L);
            }
        } finally {
            stop(process);
        }
    }

    private static long rpcThreads(JsonNode threadDump) {
        long count = 0;
        for (JsonNode thread : threadDump.get("threadInfos")) {
            if (thread.get("threadName")
                    .asText()
                    .startsWith("flink-pekko.actor.default-dispatcher-")) {
                count++;
            }
        }
        return count;
    }

    @Test
    void usesDistributedDefaultsInsteadOfMiniClusterTestDefaults() throws Exception {
        final Process process = startMiniClusterProcess("process");
        try {
            final Map<String, String> configuration = clusterConfiguration(process, "process");
            assertThat(configuration)
                    .containsEntry(RpcOptions.ASK_TIMEOUT_DURATION.key(), "10 s")
                    .containsEntry(
                            ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key(),
                            String.valueOf(4 * Hardware.getNumberCPUCores()));
        } finally {
            stop(process);
        }
    }

    @Test
    void keepsConfiguredAskTimeoutAndIoPoolSize() throws Exception {
        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                RecordingJob.class,
                                List.of(
                                        "-Dakka.ask.timeout=42 s",
                                        dynamicProperty(
                                                ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE, 7))));
        try {
            final Map<String, String> configuration = clusterConfiguration(process, "process");
            assertThat(configuration)
                    .containsEntry("akka.ask.timeout", "42 s")
                    .doesNotContainKey(RpcOptions.ASK_TIMEOUT_DURATION.key())
                    .containsEntry(ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key(), "7");
        } finally {
            stop(process);
        }
    }

    private Map<String, String> clusterConfiguration(Process process, String name)
            throws Exception {
        final Map<String, String> configuration = new HashMap<>();
        for (JsonNode entry :
                awaitRest(
                        process,
                        name,
                        "/jobmanager/config",
                        "the cluster configuration",
                        json -> true)) {
            configuration.put(entry.get("key").asText(), entry.get("value").asText());
        }
        return configuration;
    }

    @Test
    void loadsChangelogStorageFromPlugins() throws Exception {
        final Path pluginsDir = tempDir.resolve("plugins");
        writePluginJar(
                Files.createDirectories(pluginsDir.resolve("changelog-storage"))
                        .resolve("changelog-storage.jar"),
                StateChangelogStorageFactory.class,
                PluginChangelogStorageFactory.class);
        final Path storageCreated = tempDir.resolve("plugin-changelog-storage-created");

        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                UnboundedJob.class,
                                List.of(
                                        dynamicProperty(
                                                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE,
                                                PluginChangelogStorageFactory.IDENTIFIER),
                                        "-D"
                                                + PluginChangelogStorageFactory.MARKER_FILE_KEY
                                                + "="
                                                + storageCreated)),
                        Map.of(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsDir.toString()));
        try {
            awaitRest(
                    process,
                    "process",
                    "/jobs/" + jobId,
                    "the job's tasks to be running",
                    MiniClusterJobEntryPointITCase::allTasksRunning);
            assertThat(storageCreated).as("the changelog storage from the plugin is used").exists();
        } finally {
            stop(process);
        }
    }

    @Test
    void labelsFailuresWithConfiguredFailureEnrichers() throws Exception {
        final Path pluginsDir = tempDir.resolve("plugins");
        writePluginJar(
                Files.createDirectories(pluginsDir.resolve("failure-enricher"))
                        .resolve("failure-enricher.jar"),
                FailureEnricherFactory.class,
                TestFailureEnricherFactory.class,
                TestFailureEnricher.class);

        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        applicationArguments(
                                FailingTaskJob.class,
                                List.of(
                                        dynamicProperty(
                                                RestartStrategyOptions.RESTART_STRATEGY, "none"),
                                        dynamicProperty(
                                                JobManagerOptions.FAILURE_ENRICHERS_LIST,
                                                TestFailureEnricher.class.getName()))),
                        Map.of(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsDir.toString()));
        try {
            awaitJobState(process, "process", "FAILED");
            final JsonNode failure =
                    awaitRest(
                                    process,
                                    "process",
                                    "/jobs/" + jobId + "/exceptions",
                                    "the job's failure",
                                    json -> !json.at("/exceptionHistory/entries").isEmpty())
                            .at("/exceptionHistory/entries/0");
            assertThat(failure.at("/failureLabels/" + TestFailureEnricher.KEY).asText())
                    .as("the failure is labelled by the configured enricher")
                    .isEqualTo(TestFailureEnricher.VALUE);
        } finally {
            stop(process);
        }
    }

    @Test
    void providesConfiguredExternalResourcesToTasks() throws Exception {
        final Path pluginsDir = tempDir.resolve("plugins");
        writePluginJar(
                Files.createDirectories(pluginsDir.resolve("external-resource"))
                        .resolve("external-resource.jar"),
                ExternalResourceDriverFactory.class,
                TestExternalResourceDriverFactory.class,
                TestExternalResourceInfo.class);
        final String resource = TestExternalResourceDriverFactory.RESOURCE_NAME;
        final Path resourcesSeenByTask = tempDir.resolve("external-resources-seen-by-task");

        final Process process =
                startProcess(
                        "process",
                        MiniClusterJobEntryPoint.class,
                        List.of(
                                "--configDir",
                                confDir.toString(),
                                dynamicProperty(
                                        ExternalResourceOptions.EXTERNAL_RESOURCE_LIST, resource),
                                "-D"
                                        + ExternalResourceOptions.getAmountConfigOptionForResource(
                                                resource)
                                        + "=2",
                                "-D"
                                        + ExternalResourceOptions
                                                .getExternalResourceDriverFactoryConfigOptionForResource(
                                                        resource)
                                        + "="
                                        + TestExternalResourceDriverFactory.class.getName(),
                                "--job-classname",
                                ExternalResourceJob.class.getName(),
                                "--",
                                resourcesSeenByTask.toString()),
                        Map.of(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsDir.toString()));
        try {
            awaitJobState(process, "process", "FINISHED");
            assertThat(Files.readAllLines(resourcesSeenByTask))
                    .as("the task gets the configured external resources")
                    .containsExactly("device-0", "device-1");
        } finally {
            stop(process);
        }
    }

    private Process startMiniClusterProcess(String name) throws IOException {
        return startProcess(name, MiniClusterJobEntryPoint.class, applicationArguments());
    }

    private List<String> applicationArguments() {
        return applicationArguments(RecordingJob.class, List.of());
    }

    private List<String> applicationArguments(Class<?> jobClass, List<String> dynamicProperties) {
        final List<String> arguments = new ArrayList<>(List.of("--configDir", confDir.toString()));
        arguments.addAll(dynamicProperties);
        arguments.addAll(
                List.of("--job-classname", jobClass.getName(), "--", markerFile.toString()));
        return arguments;
    }

    private Process startProcess(String name, Class<?> mainClass, List<String> arguments)
            throws IOException {
        return startProcess(name, mainClass, arguments, Map.of());
    }

    private Process startProcess(
            String name,
            Class<?> mainClass,
            List<String> arguments,
            Map<String, String> environment)
            throws IOException {
        final List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx512m");
        command.add("-Dlog4j.configurationFile=" + log4jConfig.toUri());
        command.add("-classpath");
        command.add(System.getProperty("java.class.path"));
        // registering the DataStream transformation translators reflects into a final map
        command.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        // the JMX server's registry (as in the distribution's default env.java.opts.all)
        command.add("--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED");
        command.add(mainClass.getName());
        command.addAll(arguments);

        final ProcessBuilder processBuilder =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(logFile(name).toFile());
        processBuilder.environment().putAll(environment);
        return processBuilder.start();
    }

    private void awaitJobState(Process process, String name, String expectedState)
            throws Exception {
        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Optional<String> lastState = Optional.empty();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                printLog(name);
                fail(
                        "The %s MiniCluster process exited with code %d"
                                + " while waiting for job %s to reach %s"
                                + " (last seen: %s).",
                        name, process.exitValue(), jobId, expectedState, lastState.orElse("none"));
            }
            lastState = queryJobState();
            if (lastState.filter(expectedState::equals).isPresent()) {
                return;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        printLog(name);
        fail(
                "Job %s did not reach %s in the %s process within %s (last seen: %s).",
                jobId, expectedState, name, TIMEOUT, lastState.orElse("none"));
    }

    private Optional<String> queryJobState() throws InterruptedException {
        return queryRest("/jobs/" + jobId).map(json -> json.get("state").asText());
    }

    private JsonNode awaitRest(
            Process process,
            String name,
            String path,
            String description,
            Predicate<JsonNode> condition)
            throws Exception {
        final Optional<JsonNode> result =
                pollRest(process, path, condition, System.nanoTime() + TIMEOUT.toNanos());
        if (result.isEmpty()) {
            printLog(name);
            fail(
                    "The %s process exited with code %d while waiting for %s.",
                    name, process.exitValue(), description);
        }
        return result.get();
    }

    /**
     * Polls {@code path} until {@code condition} holds, returning empty if the process exits first,
     * and failing if the deadline passes.
     */
    private Optional<JsonNode> pollRest(
            Process process, String path, Predicate<JsonNode> condition, long deadlineNanos)
            throws Exception {
        Optional<JsonNode> last = Optional.empty();
        while (System.nanoTime() < deadlineNanos) {
            if (!process.isAlive()) {
                return Optional.empty();
            }
            last = queryRest(path);
            if (last.filter(condition).isPresent()) {
                return last;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        return fail(
                "%s did not satisfy the condition in time (last: %s).", path, last.orElse(null));
    }

    /** Waits until the dispatcher has leadership and serves requests. */
    private void awaitClusterServing(Process process, String name) throws Exception {
        awaitRest(process, name, "/overview", "the cluster overview", json -> true);
    }

    private void assertStaysUp(Process process, String name) throws Exception {
        final long deadline = System.nanoTime() + STAYS_UP_WINDOW.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                printLog(name);
                fail(
                        "The %s process exited with code %d within %s of serving requests.",
                        name, process.exitValue(), STAYS_UP_WINDOW);
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
    }

    private Optional<JsonNode> queryRest(String path) throws InterruptedException {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create("http://localhost:" + restPort + path))
                        .timeout(REQUEST_TIMEOUT)
                        .GET()
                        .build();
        try {
            final HttpResponse<String> response =
                    HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return Optional.empty();
            }
            return Optional.of(OBJECT_MAPPER.readTree(response.body()));
        } catch (IOException e) {
            // REST endpoint not up yet
            return Optional.empty();
        }
    }

    private JsonNode postRest(String path, JsonNode body) throws Exception {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create("http://localhost:" + restPort + path))
                        .timeout(REQUEST_TIMEOUT)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                        .build();
        final HttpResponse<String> response =
                HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as("POST %s returned %s", path, response.body())
                .isBetween(200, 299);
        return OBJECT_MAPPER.readTree(response.body());
    }

    private String checkpointsPath() {
        return "/jobs/" + jobId + "/checkpoints";
    }

    private static boolean allTasksRunning(JsonNode job) {
        final JsonNode vertices = job.get("vertices");
        if (vertices == null || vertices.isEmpty()) {
            return false;
        }
        for (JsonNode vertex : vertices) {
            if (!"RUNNING".equals(vertex.get("status").asText())) {
                return false;
            }
        }
        return true;
    }

    private static void stop(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly().waitFor();
        }
    }

    private int awaitExit(Process process, String name) throws Exception {
        if (!process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            printLog(name);
            fail("The %s process did not exit within %s.", name, TIMEOUT);
        }
        return process.exitValue();
    }

    private void awaitFile(Process process, String name, Path file) throws Exception {
        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(file)) {
                return;
            }
            if (!process.isAlive()) {
                printLog(name);
                fail(
                        "The %s process exited with code %d while waiting for %s.",
                        name, process.exitValue(), file);
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        printLog(name);
        fail("%s was not created by the %s process within %s.", file, name, TIMEOUT);
    }

    private boolean zooKeeperNodeExists(String path) throws Exception {
        return ZOOKEEPER
                        .getCustomExtension()
                        .getZooKeeperClient(new TestingFatalErrorHandler())
                        .usingNamespace(null)
                        .checkExists()
                        .forPath(path)
                != null;
    }

    private String executionPlanZooKeeperPath() {
        return "/flink/" + haClusterId + "/execution-plans/" + jobId;
    }

    /** Writes a jar holding {@code mainClass}, named as the entry class in its manifest. */
    private static void writeJar(Path jar, Class<?> mainClass) throws IOException {
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, mainClass.getName());
        final String entry = mainClass.getName().replace('.', '/') + ".class";
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), manifest);
                InputStream classBytes = mainClass.getClassLoader().getResourceAsStream(entry)) {
            out.putNextEntry(new JarEntry(entry));
            classBytes.transferTo(out);
            out.closeEntry();
        }
    }

    private static void writePluginJar(
            Path jar, Class<?> service, Class<?> implementation, Class<?>... otherClasses)
            throws IOException {
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            for (Class<?> pluginClass :
                    Stream.concat(Stream.of(implementation), Stream.of(otherClasses))
                            .toArray(Class<?>[]::new)) {
                final String entry = pluginClass.getName().replace('.', '/') + ".class";
                try (InputStream classBytes =
                        pluginClass.getClassLoader().getResourceAsStream(entry)) {
                    out.putNextEntry(new JarEntry(entry));
                    classBytes.transferTo(out);
                    out.closeEntry();
                }
            }
            out.putNextEntry(new JarEntry("META-INF/services/" + service.getName()));
            out.write(implementation.getName().getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
    }

    private long mainInvocations() throws IOException {
        return Files.exists(markerFile) ? Files.readAllLines(markerFile).size() : 0;
    }

    private Path logFile(String name) {
        return tempDir.resolve(name + ".log");
    }

    private void printLog(String name) throws IOException {
        System.out.println("===== " + name + " process log =====");
        System.out.println(Files.readString(logFile(name)));
    }

    private static String dynamicProperty(ConfigOption<?> option, Object value) {
        return "-D" + option.key() + "=" + value;
    }

    private static String entry(ConfigOption<?> option, Object value) {
        return option.key() + ": " + value;
    }

    /** Creates HA services that fail to clean up their data. */
    public static final class FailingCleanupHaServicesFactory
            implements HighAvailabilityServicesFactory {

        static final String FAILURE_MESSAGE = "Could not clean up the HA data.";

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            return new EmbeddedHaServices(executor) {
                @Override
                public void cleanupAllData() throws Exception {
                    throw new IOException(FAILURE_MESSAGE);
                }
            };
        }
    }

    /**
     * Creates HA services whose cleanup takes a while, writing marker files to the directory given
     * by {@link #MARKER_DIR} as it starts and finishes.
     */
    public static final class SlowCleanupHaServicesFactory
            implements HighAvailabilityServicesFactory {

        static final ConfigOption<String> MARKER_DIR =
                ConfigOptions.key("test.ha-cleanup-marker-dir").stringType().noDefaultValue();
        static final String STARTED = "ha-cleanup-started";
        static final String FINISHED = "ha-cleanup-finished";

        /**
         * Long enough to send SIGTERM during, and short enough that it and the rest of the shutdown
         * finish well within the shutdown hook's timeout, even on a slow machine.
         */
        private static final Duration CLEANUP_DURATION = Duration.ofSeconds(1);

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            final Path markerDir = Path.of(configuration.get(MARKER_DIR));
            return new EmbeddedHaServices(executor) {
                @Override
                public void cleanupAllData() throws Exception {
                    Files.createFile(markerDir.resolve(STARTED));
                    Thread.sleep(CLEANUP_DURATION.toMillis());
                    super.cleanupAllData();
                    Files.createFile(markerDir.resolve(FINISHED));
                }
            };
        }
    }

    public static final class RecordingJob {

        public static void main(String[] args) throws Exception {
            Files.writeString(
                    Path.of(args[0]),
                    "main()" + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSequence(1, 10).sinkTo(new DiscardingSink<>());
            env.execute();
        }
    }

    /** Writes the ids of the external resources its task gets to the file given as argument. */
    public static final class ExternalResourceJob {

        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSequence(1, 1)
                    .map(new RecordExternalResources(args[0]))
                    .sinkTo(new DiscardingSink<>());
            env.execute();
        }
    }

    private static final class RecordExternalResources extends RichMapFunction<Long, Long> {

        private final String outputFile;

        RecordExternalResources(String outputFile) {
            this.outputFile = outputFile;
        }

        @Override
        public void open(OpenContext openContext) throws IOException {
            final List<String> ids =
                    getRuntimeContext()
                            .getExternalResourceInfos(
                                    TestExternalResourceDriverFactory.RESOURCE_NAME)
                            .stream()
                            .map(info -> info.getProperty("id").orElseThrow())
                            .sorted()
                            .collect(Collectors.toList());
            Files.write(Path.of(outputFile), ids);
        }

        @Override
        public Long map(Long value) {
            return value;
        }
    }

    public static final class FailingTaskJob {

        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSequence(1, 1).map(FailingTaskJob::fail).sinkTo(new DiscardingSink<>());
            env.execute();
        }

        private static long fail(long value) {
            throw new IllegalStateException("The task fails.");
        }
    }

    public static final class FailingJob {

        public static void main(String[] args) throws Exception {
            Files.writeString(
                    Path.of(args[0]),
                    "main()" + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            throw new IllegalStateException("The application fails before submitting a job.");
        }
    }

    /**
     * A job that can't be cancelled: one task ignores interrupts, and another fails once the first
     * is stuck, so the failover's cancellation times out and the TaskManager hits a fatal error.
     */
    public static final class StuckOnCancelJob {

        private static volatile boolean stuck;

        public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSequence(1, Long.MAX_VALUE)
                    .map(StuckOnCancelJob::failOnceStuck)
                    .disableChaining()
                    .map(StuckOnCancelJob::ignoreInterrupts)
                    .disableChaining()
                    .sinkTo(new DiscardingSink<>());
            env.execute();
        }

        private static long failOnceStuck(long value) {
            if (stuck) {
                throw new IllegalStateException("Failing so that the stuck task is cancelled.");
            }
            return value;
        }

        private static long ignoreInterrupts(long value) {
            stuck = true;
            while (true) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // ignored, like user code that doesn't respond to cancellation
                }
            }
        }
    }

    public static final class UnboundedJob {

        public static void main(String[] args) throws Exception {
            Files.writeString(
                    Path.of(args[0]),
                    "main()" + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.fromSource(
                            new DataGeneratorSource<>(
                                    index -> index,
                                    Long.MAX_VALUE,
                                    RateLimiterStrategy.perSecond(10),
                                    Types.LONG),
                            WatermarkStrategy.noWatermarks(),
                            "generator")
                    .sinkTo(new DiscardingSink<>());
            env.execute();
        }
    }

    /**
     * An in-memory changelog storage under another name, which records that it was created. It's
     * only registered in a plugin jar, not on the classpath.
     */
    public static final class PluginChangelogStorageFactory
            extends InMemoryStateChangelogStorageFactory {

        static final String IDENTIFIER = "plugin";
        static final String MARKER_FILE_KEY = "test.plugin-changelog-storage.marker-file";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public StateChangelogStorage<?> createStorage(
                JobID jobID,
                Configuration configuration,
                TaskManagerJobMetricGroup metricGroup,
                LocalRecoveryConfig localRecoveryConfig) {
            try {
                Files.writeString(Path.of(configuration.toMap().get(MARKER_FILE_KEY)), "");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return super.createStorage(jobID, configuration, metricGroup, localRecoveryConfig);
        }
    }

    /** Creates {@link TestFailureEnricher}. It's only registered in a plugin jar. */
    public static final class TestFailureEnricherFactory implements FailureEnricherFactory {

        @Override
        public FailureEnricher createFailureEnricher(Configuration conf) {
            return new TestFailureEnricher();
        }
    }

    /** Labels every failure with the same label. */
    public static final class TestFailureEnricher implements FailureEnricher {

        static final String KEY = "test-label";
        static final String VALUE = "labelled";

        @Override
        public Set<String> getOutputKeys() {
            return Set.of(KEY);
        }

        @Override
        public CompletableFuture<Map<String, String>> processFailure(
                Throwable cause, Context context) {
            return CompletableFuture.completedFuture(Map.of(KEY, VALUE));
        }
    }

    /** Provides as many devices as requested. It's only registered in a plugin jar. */
    public static final class TestExternalResourceDriverFactory
            implements ExternalResourceDriverFactory {

        static final String RESOURCE_NAME = "test-device";

        @Override
        public ExternalResourceDriver createExternalResourceDriver(Configuration config) {
            return amount ->
                    LongStream.range(0, amount)
                            .mapToObj(index -> new TestExternalResourceInfo("device-" + index))
                            .collect(Collectors.toSet());
        }
    }

    private static final class TestExternalResourceInfo implements ExternalResourceInfo {

        private final String id;

        TestExternalResourceInfo(String id) {
            this.id = id;
        }

        @Override
        public Optional<String> getProperty(String key) {
            return "id".equals(key) ? Optional.of(id) : Optional.empty();
        }

        @Override
        public Collection<String> getKeys() {
            return List.of("id");
        }
    }
}
