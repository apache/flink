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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MdcUtils;

import org.apache.logging.log4j.core.LogEvent;
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
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.event.Level.DEBUG;

/**
 * Tests adding of {@link JobID} to logs (via {@link org.slf4j.MDC}) in the most important cases.
 */
public class JobIDLoggingITCase {
    private static final Logger logger = LoggerFactory.getLogger(JobIDLoggingITCase.class);

    @RegisterExtension
    public final LoggerAuditingExtension checkpointCoordinatorLogging =
            new LoggerAuditingExtension(CheckpointCoordinator.class, DEBUG);

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
    public final LoggerAuditingExtension asyncCheckpointRunnableLogging =
            // this class is private
            new LoggerAuditingExtension(
                    "org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable", DEBUG);

    @RegisterExtension
    public static MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void testJobIDLogging(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        JobID jobID = runJob(clusterClient);
        clusterClient.cancel(jobID).get();

        // NOTE: most of the assertions are empirical, such as
        // - which classes are important
        // - how many messages to expect
        // - which log patterns to ignore

        assertJobIDPresent(jobID, 3, checkpointCoordinatorLogging);
        assertJobIDPresent(jobID, 6, streamTaskLogging);
        assertJobIDPresent(
                jobID,
                9,
                taskExecutorLogging,
                "Un-registering task.*",
                "Successful registration.*",
                "Establish JobManager connection.*",
                "Offer reserved slots.*",
                ".*ResourceManager.*",
                "Operator event.*");

        assertJobIDPresent(jobID, 10, taskLogging);
        assertJobIDPresent(jobID, 10, executionGraphLogging);
        assertJobIDPresent(
                jobID,
                15,
                jobMasterLogging,
                "Registration at ResourceManager.*",
                "Registration with ResourceManager.*",
                "Resolved ResourceManager address.*");
        assertJobIDPresent(jobID, 1, asyncCheckpointRunnableLogging);
    }

    private static void assertJobIDPresent(
            JobID jobID,
            int expectedLogMessages,
            LoggerAuditingExtension ext,
            String... ignPatterns) {
        String loggerName = ext.getLoggerName();
        checkState(
                ext.getEvents().size() >= expectedLogMessages,
                "Too few log events recorded for %s (%s) - this must be a bug in the test code",
                loggerName,
                ext.getEvents().size());

        final List<LogEvent> eventsWithMissingJobId = new ArrayList<>();
        final List<LogEvent> eventsWithWrongJobId = new ArrayList<>();
        final List<LogEvent> ignoredEvents = new ArrayList<>();
        final List<Pattern> ignorePatterns =
                Arrays.stream(ignPatterns).map(Pattern::compile).collect(Collectors.toList());

        for (LogEvent e : ext.getEvents()) {
            if (e.getContextData().containsKey(MdcUtils.JOB_ID)) {
                if (!Objects.equals(
                        e.getContextData().getValue(MdcUtils.JOB_ID), jobID.toHexString())) {
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
                loggerName,
                ext.getEvents(),
                ignoredEvents,
                eventsWithWrongJobId,
                eventsWithMissingJobId);
        assertThat(eventsWithWrongJobId).as("events with a wrong Job ID").isEmpty();
        assertTrue(
                eventsWithMissingJobId.isEmpty(),
                "too many events without Job ID recorded for "
                        + loggerName
                        + ": "
                        + eventsWithMissingJobId);
    }

    private static boolean matchesAny(List<Pattern> patternStream, String message) {
        return patternStream.stream().anyMatch(p -> p.matcher(message).matches());
    }

    private static JobID runJob(ClusterClient<?> clusterClient) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE).addSink(new DiscardingSink<>());
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
