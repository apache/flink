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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobRetrievalSupportedPipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Controls the continuous refresh jobs of materialized tables. */
@Internal
final class RefreshJobController {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshJobController.class);

    static final ConfigOption<Duration> CLIENT_TIMEOUT =
            ConfigOptions.key("client.timeout").durationType().defaultValue(Duration.ofSeconds(60));

    private static final String CONFIGURED_COORDINATES_HINT =
            "The job was looked up with coordinates the catalog does not record, such as "
                    + "'rest.address', 'kubernetes.namespace' and high-availability settings, "
                    + "which are taken from this program's configuration and must point at the "
                    + "cluster the job runs on.";

    private static final String REQUEST_MAY_STILL_COMPLETE =
            "The request may still complete on the cluster, so check the status of the refresh "
                    + "job before retrying.";

    private static final String SAVEPOINT_RECOVERY_HINT =
            "If the job still stops, its savepoint is written under '%s' while the materialized "
                    + "table stays ACTIVATED; set 'execution.state-recovery.path' to that savepoint "
                    + "before resuming, and unset it afterwards (RESET in SQL), otherwise RESUME "
                    + "starts the refresh job without restoring state.";

    private final TableConfig tableConfig;
    private final ClassLoader userClassLoader;
    private final Function<Configuration, Optional<JobRetrievalSupportedPipelineExecutor>>
            executorLookup;

    private final Map<String, JobClient> submittedJobClients = new HashMap<>();
    private final Map<String, JobClient> retrievedJobClients = new HashMap<>();

    RefreshJobController(TableConfig tableConfig, ClassLoader userClassLoader) {
        this(tableConfig, userClassLoader, RefreshJobController::findRetrievalExecutor);
    }

    @VisibleForTesting
    RefreshJobController(
            TableConfig tableConfig,
            ClassLoader userClassLoader,
            Function<Configuration, Optional<JobRetrievalSupportedPipelineExecutor>>
                    executorLookup) {
        this.tableConfig = checkNotNull(tableConfig);
        this.userClassLoader = checkNotNull(userClassLoader);
        this.executorLookup = checkNotNull(executorLookup);
    }

    void register(RefreshJobResult result) {
        result.getJobClient()
                .ifPresent(jobClient -> submittedJobClients.put(result.getJobId(), jobClient));
    }

    JobStatus getJobStatus(ContinuousRefreshHandler refreshHandler) {
        final JobStatus jobStatus;
        try {
            jobStatus = await(refreshHandler, JobRequest.GET_STATUS, JobClient::getJobStatus, "");
        } catch (TableException e) {
            if (isUnknownJob(e)) {
                throw new TableException(
                        String.format(
                                "Described job %s does not exist in the cluster.",
                                refreshHandler.getJobId()));
            }
            throw e;
        }
        if (jobStatus.isGloballyTerminalState()) {
            submittedJobClients.remove(refreshHandler.getJobId());
        }
        return jobStatus;
    }

    void cancel(ContinuousRefreshHandler refreshHandler) {
        await(refreshHandler, JobRequest.CANCEL, JobClient::cancel, "");
        submittedJobClients.remove(refreshHandler.getJobId());
    }

    String stopWithSavepoint(ContinuousRefreshHandler refreshHandler, String savepointDirectory) {
        final String savepointPath =
                await(
                        refreshHandler,
                        JobRequest.STOP_WITH_SAVEPOINT,
                        jobClient ->
                                jobClient.stopWithSavepoint(
                                        false, savepointDirectory, SavepointFormatType.DEFAULT),
                        String.format(SAVEPOINT_RECOVERY_HINT, savepointDirectory));
        submittedJobClients.remove(refreshHandler.getJobId());
        return savepointPath;
    }

    void releaseRetrievedJobClients() {
        for (JobClient jobClient : retrievedJobClients.values()) {
            if (jobClient instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) jobClient).close();
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to close the client of refresh job {}.",
                            jobClient.getJobID(),
                            e);
                }
            }
        }
        retrievedJobClients.clear();
    }

    private JobClient lookup(ContinuousRefreshHandler refreshHandler) {
        final JobClient submittedJobClient = submittedJobClients.get(refreshHandler.getJobId());
        if (submittedJobClient != null) {
            return submittedJobClient;
        }
        final JobClient cachedJobClient = retrievedJobClients.get(refreshHandler.getJobId());
        if (cachedJobClient != null) {
            return cachedJobClient;
        }

        final Optional<JobClient> retrievedJobClient;
        try {
            retrievedJobClient = retrieve(refreshHandler);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new TableException(
                    "Failed to run cluster action.", ExceptionUtils.stripExecutionException(e));
        }
        final JobClient jobClient =
                retrievedJobClient.orElseThrow(
                        () -> noRetrievalExecutor(refreshHandler, retrievalTarget(refreshHandler)));
        retrievedJobClients.put(refreshHandler.getJobId(), jobClient);
        return jobClient;
    }

    private Optional<JobClient> retrieve(ContinuousRefreshHandler refreshHandler) throws Exception {
        final Configuration retrievalConfiguration = programConfiguration();
        RefreshJobTarget.recordedIn(refreshHandler).applyTo(retrievalConfiguration);
        retrievalConfiguration.set(DeploymentOptions.TARGET, retrievalTarget(refreshHandler));
        final Optional<JobRetrievalSupportedPipelineExecutor> executor =
                executorLookup.apply(retrievalConfiguration);
        if (executor.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                executor.get()
                        .retrieveJobClient(
                                JobID.fromHexString(refreshHandler.getJobId()),
                                retrievalConfiguration,
                                userClassLoader)
                        .get());
    }

    private static Optional<JobRetrievalSupportedPipelineExecutor> findRetrievalExecutor(
            Configuration configuration) {
        final PipelineExecutor executor;
        try {
            executor =
                    new DefaultExecutorServiceLoader()
                            .getExecutorFactory(configuration)
                            .getExecutor(configuration);
        } catch (IllegalStateException e) {
            LOG.info(
                    "No usable pipeline executor for execution target '{}'.",
                    configuration.get(DeploymentOptions.TARGET));
            return Optional.empty();
        }
        return executor instanceof JobRetrievalSupportedPipelineExecutor
                ? Optional.of((JobRetrievalSupportedPipelineExecutor) executor)
                : Optional.empty();
    }

    private static String retrievalTarget(ContinuousRefreshHandler refreshHandler) {
        final String recordedTarget = refreshHandler.getExecutionTarget();
        if (RefreshJobTarget.recordedIn(refreshHandler).isApplicationTarget()) {
            return recordedTarget.substring(0, recordedTarget.length() - "application".length())
                    + "session";
        }
        return recordedTarget;
    }

    private Configuration programConfiguration() {
        final Configuration configuration =
                Configuration.fromMap(tableConfig.getRootConfiguration().toMap());
        configuration.addAll(tableConfig.getConfiguration());
        return configuration;
    }

    private static boolean isUnknownJob(Throwable failure) {
        return ExceptionUtils.findThrowable(failure, FlinkJobNotFoundException.class).isPresent()
                || ExceptionUtils.findThrowableWithMessage(
                                failure, FlinkJobNotFoundException.class.getName())
                        .isPresent();
    }

    /** Sends a request to the job and waits for its result. */
    private <T> T await(
            ContinuousRefreshHandler refreshHandler,
            JobRequest jobRequest,
            Function<JobClient, CompletableFuture<T>> request,
            String recoveryHint) {
        final String action = jobRequest.action;
        final String coordinatesHint =
                submittedJobClients.containsKey(refreshHandler.getJobId())
                        ? ""
                        : CONFIGURED_COORDINATES_HINT;
        final JobClient jobClient = lookup(refreshHandler);

        final CompletableFuture<T> response;
        try {
            response = request.apply(jobClient);
        } catch (Exception e) {
            if (jobRequest == JobRequest.GET_STATUS) {
                throw statusLookupFailure(refreshHandler, e);
            }
            throw new TableException(
                    withHints(failureMessage(action, refreshHandler), coordinatesHint), e);
        }

        final String pendingRequestHint =
                jobRequest.mayStillComplete
                        ? withHints(REQUEST_MAY_STILL_COMPLETE, recoveryHint)
                        : "";
        final Duration timeout = tableConfig.get(CLIENT_TIMEOUT);
        try {
            return response.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            final TableException timeoutFailure =
                    new TableException(
                            withHints(
                                    timeoutMessage(action, refreshHandler, timeout),
                                    coordinatesHint,
                                    pendingRequestHint),
                            e);
            throw jobRequest == JobRequest.GET_STATUS
                    ? statusLookupFailure(refreshHandler, timeoutFailure)
                    : timeoutFailure;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            if (jobRequest == JobRequest.GET_STATUS) {
                throw statusLookupFailure(refreshHandler, e);
            }
            throw new TableException(
                    withHints(
                            failureMessage(action, refreshHandler),
                            coordinatesHint,
                            pendingRequestHint),
                    e);
        }
    }

    private static TableException statusLookupFailure(
            ContinuousRefreshHandler refreshHandler, Throwable cause) {
        return new TableException(
                String.format("Failed to get job %s in the cluster.", refreshHandler.getJobId()),
                cause);
    }

    private static String failureMessage(String action, ContinuousRefreshHandler refreshHandler) {
        return String.format("Failed to %s.", describe(action, refreshHandler));
    }

    private static TableException noRetrievalExecutor(
            ContinuousRefreshHandler refreshHandler, String lookedUpTarget) {
        return new TableException(
                String.format(
                        "Cannot reach refresh job %s: no pipeline executor that can reach "
                                + "existing jobs supports execution target '%s'%s. Either the "
                                + "executor for this target is missing from this program's class "
                                + "path (flink-clients, plus flink-yarn with HADOOP_CLASSPATH or "
                                + "flink-kubernetes), more than one PipelineExecutorFactory on the "
                                + "class path supports the target, or jobs on this target can only "
                                + "be controlled from the TableEnvironment that submitted them.",
                        refreshHandler.getJobId(),
                        lookedUpTarget,
                        lookedUpTarget.equals(refreshHandler.getExecutionTarget())
                                ? ""
                                : String.format(
                                        " (used for the recorded target '%s')",
                                        refreshHandler.getExecutionTarget())));
    }

    private static String timeoutMessage(
            String action, ContinuousRefreshHandler refreshHandler, Duration timeout) {
        return String.format(
                "Timed out after %s ('%s') waiting to %s.",
                TimeUtils.formatWithHighestUnit(timeout),
                CLIENT_TIMEOUT.key(),
                describe(action, refreshHandler));
    }

    private static String withHints(String message, String... hints) {
        final StringBuilder messageWithHints = new StringBuilder(message);
        for (String hint : hints) {
            if (!hint.isEmpty()) {
                messageWithHints.append(' ').append(hint);
            }
        }
        return messageWithHints.toString();
    }

    private static String describe(String action, ContinuousRefreshHandler refreshHandler) {
        return String.format(
                "%s (execution.target=%s, cluster id=%s)",
                String.format(action, refreshHandler.getJobId()),
                refreshHandler.getExecutionTarget(),
                RefreshJobTarget.recordedIn(refreshHandler).getClusterId());
    }

    private enum JobRequest {
        GET_STATUS("get the status of refresh job %s", false),
        CANCEL("cancel refresh job %s", true),
        STOP_WITH_SAVEPOINT("stop refresh job %s with a savepoint", true);

        private final String action;
        private final boolean mayStillComplete;

        JobRequest(String action, boolean mayStillComplete) {
            this.action = action;
            this.mayStillComplete = mayStillComplete;
        }
    }
}
