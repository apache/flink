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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.ExceptionUtils;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.configuration.JobManagerOptions.SCHEDULER;
import static org.apache.flink.util.MdcUtils.JOB_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.DEBUG;

class JobIDLoggingITCase {
    private static final Logger logger = LoggerFactory.getLogger(JobIDLoggingITCase.class);

    @RegisterExtension
    public final LoggerAuditingExtension checkpointCoordinatorLogging =
            new LoggerAuditingExtension(CheckpointCoordinator.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension sourceCoordinatorLogging =
            new LoggerAuditingExtension(SourceCoordinator.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension streamTaskLogging =
            new LoggerAuditingExtension(StreamTask.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension taskExecutorLogging =
            new LoggerAuditingExtension(TaskExecutor.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension taskLogging =
            new LoggerAuditingExtension(Task.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension executionGraphLogging =
            new LoggerAuditingExtension(ExecutionGraph.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension jobMasterLogging =
            new LoggerAuditingExtension(JobMaster.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension adaptiveSchedulerLogging =
            new LoggerAuditingExtension(AdaptiveScheduler.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension asyncCheckpointRunnableLogging =
            // this class is private
            new LoggerAuditingExtension(
                    "org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable", DEBUG);

    @RegisterExtension
    public static MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        return configuration;
    }

    @Test
    void testJobIDLogging(@InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        JobID jobID = runJob(clusterClient);
        clusterClient.cancel(jobID).get();

        // NOTE: most of the assertions are empirical, such as
        // - which classes are important
        // - how many messages to expect
        // - which log patterns to ignore

        assertJobIDPresent(
                jobID,
                checkpointCoordinatorLogging,
                asList(
                        "No checkpoint found during restore.",
                        "Resetting the master hooks.",
                        "Triggering checkpoint .*",
                        "Received acknowledge message for checkpoint .*",
                        "Completed checkpoint .*",
                        "Checkpoint state: .*"));

        assertJobIDPresent(
                jobID,
                sourceCoordinatorLogging,
                asList(
                        "Starting split enumerator.*",
                        "Distributing maxAllowedWatermark.*",
                        "Source .* registering reader for parallel task.*",
                        "Closing SourceCoordinator for source .*"));

        assertJobIDPresent(
                jobID,
                streamTaskLogging,
                asList(
                        "State backend is set to .*",
                        "Initializing Source: .*",
                        "Invoking Source: .*",
                        "Starting checkpoint .*",
                        "Notify checkpoint \\d+ complete .*"));

        assertJobIDPresent(
                jobID,
                taskExecutorLogging,
                asList(
                        "Received task .*",
                        "Trigger checkpoint .*",
                        "Confirm completed checkpoint .*"),
                "TaskManager received a checkpoint confirmation for unknown task.*",
                "TaskManager received an aborted checkpoint for unknown task.*",
                "Un-registering task.*",
                "Successful registration.*",
                "Establish JobManager connection.*",
                "Offer reserved slots.*",
                ".*ResourceManager.*",
                "Operator event.*",
                "Recovered slot allocation snapshots.*",
                ".*heartbeat.*",
                ".*leadership.*");

        assertJobIDPresent(
                jobID,
                taskLogging,
                asList(
                        "Source: .* switched from CREATED to DEPLOYING.",
                        "Source: .* switched from DEPLOYING to INITIALIZING.",
                        "Source: .* switched from INITIALIZING to RUNNING."));

        assertJobIDPresent(
                jobID,
                executionGraphLogging,
                asList(
                        "Created execution graph .*",
                        "Deploying Source.*",
                        "Job .* switched from state CREATED to RUNNING.",
                        "Source: .* switched from CREATED to SCHEDULED.",
                        "Source: .* switched from SCHEDULED to DEPLOYING.",
                        "Source: .* switched from DEPLOYING to INITIALIZING.",
                        "Source: .* switched from INITIALIZING to RUNNING."));

        assertJobIDPresent(
                jobID,
                adaptiveSchedulerLogging,
                asList(
                        "Checkpoint storage is set to .*",
                        "Running initialization on master for job .*",
                        "Successfully created execution graph from job graph .*",
                        "Successfully ran initialization on master.*"),
                "Registration at ResourceManager.*",
                "Registration with ResourceManager.*",
                "Resolved ResourceManager address.*");

        assertJobIDPresent(
                jobID,
                jobMasterLogging,
                asList(
                        "Initializing job .*",
                        "Starting execution of job .*",
                        "Using restart back off time strategy .*"),
                "Registration at ResourceManager.*",
                "Registration with ResourceManager.*",
                "Resolved ResourceManager address.*");

        assertJobIDPresent(
                jobID,
                asyncCheckpointRunnableLogging,
                asList(
                        ".* started executing asynchronous part of checkpoint .*",
                        ".* finished asynchronous part of checkpoint .*"));
    }

    private static void assertJobIDPresent(
            JobID jobID,
            LoggerAuditingExtension ext,
            List<String> expPatterns,
            String... ignPatterns) {

        final List<LogEvent> eventsWithMissingJobId = new ArrayList<>();
        final List<LogEvent> eventsWithWrongJobId = new ArrayList<>();
        final List<LogEvent> ignoredEvents = new ArrayList<>();
        final List<Pattern> expectedPatterns =
                expPatterns.stream().map(Pattern::compile).collect(toList());
        final List<Pattern> ignorePatterns =
                Arrays.stream(ignPatterns).map(Pattern::compile).collect(toList());

        for (LogEvent e : ext.getEvents()) {
            ReadOnlyStringMap context = e.getContextData();
            if (context.containsKey(JOB_ID)) {
                if (Objects.equals(context.getValue(JOB_ID), jobID.toHexString())) {
                    expectedPatterns.removeIf(
                            pattern ->
                                    pattern.matcher(e.getMessage().getFormattedMessage())
                                            .matches());
                } else {
                    eventsWithWrongJobId.add(e);
                }
            } else if (matchesAny(ignorePatterns, e.getMessage().getFormattedMessage())) {
                ignoredEvents.add(e);
            } else {
                eventsWithMissingJobId.add(e);
            }
        }

        logger.debug(
                "checked events for {}:\n  {};\n  ignored: {},\n  wrong job id: {},\n  missing job id: {}",
                ext.getLoggerName(),
                ext.getEvents(),
                ignoredEvents,
                eventsWithWrongJobId,
                eventsWithMissingJobId);
        assertThat(eventsWithWrongJobId).as("events with a wrong Job ID").isEmpty();
        assertThat(expectedPatterns)
                .as(
                        "not all expected events logged by %s, logged:\n%s",
                        ext.getLoggerName(), ext.getEvents())
                .isEmpty();
        assertThat(eventsWithMissingJobId)
                .as("too many events without Job ID logged by %s", ext.getLoggerName())
                .isEmpty();
    }

    private static boolean matchesAny(List<Pattern> patternStream, String message) {
        return patternStream.stream().anyMatch(p -> p.matcher(message).matches());
    }

    private static JobID runJob(ClusterClient<?> clusterClient) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.forGenerator(ctx -> new AscendingTimestampsWatermarks())
                                .withWatermarkAlignment(
                                        "group-1", Duration.ofMillis(1000), Duration.ofMillis(1))
                                .withTimestampAssigner((r, t) -> (long) r),
                        "Source-42441337")
                .addSink(new DiscardingSink<>());
        JobID jobId = clusterClient.submitJob(env.getStreamGraph().getJobGraph()).get();
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));
        while (deadline.hasTimeLeft()
                && clusterClient.listJobs().get().stream()
                        .noneMatch(
                                m ->
                                        m.getJobId().equals(jobId)
                                                && m.getJobState().equals(JobStatus.RUNNING))) {
            Thread.sleep(10);
        }
        // wait for all tasks ready and then checkpoint
        while (true) {
            try {
                clusterClient.triggerCheckpoint(jobId, CheckpointType.DEFAULT).get();
                // to check the log message about checkpoint completion notification we need to
                // either wait or trigger another checkpoint
                clusterClient.triggerCheckpoint(jobId, CheckpointType.DEFAULT).get();
                return jobId;
            } catch (ExecutionException e) {
                if (ExceptionUtils.findThrowable(e, CheckpointException.class).isPresent()
                        && !deadline.isOverdue()) {
                    Thread.sleep(10);
                } else {
                    throw e;
                }
            }
        }
    }
}
