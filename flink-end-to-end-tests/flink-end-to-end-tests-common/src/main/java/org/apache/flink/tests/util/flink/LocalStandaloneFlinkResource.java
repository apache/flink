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

package org.apache.flink.tests.util.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Flink resource that start local standalone clusters. */
public class LocalStandaloneFlinkResource implements FlinkResource {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneFlinkResource.class);

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Path distributionDirectory;
    @Nullable private final Path logBackupDirectory;
    private final FlinkResourceSetup setup;

    private FlinkDistribution distribution;

    LocalStandaloneFlinkResource(
            Path distributionDirectory,
            @Nullable Path logBackupDirectory,
            FlinkResourceSetup setup) {
        LOG.info("Using distribution {}.", distributionDirectory);
        this.distributionDirectory = distributionDirectory;
        this.logBackupDirectory = logBackupDirectory;
        this.setup = setup;
    }

    @Override
    public void before() throws Exception {
        temporaryFolder.create();
        Path tmp = temporaryFolder.newFolder().toPath();
        LOG.info("Copying distribution to {}.", tmp);
        TestUtils.copyDirectory(distributionDirectory, tmp);

        distribution = new FlinkDistribution(tmp);
        distribution.setRootLogLevel(Level.DEBUG);
        for (JarOperation jarOperation : setup.getJarOperations()) {
            distribution.performJarOperation(jarOperation);
        }
        if (setup.getConfig().isPresent()) {
            distribution.appendConfiguration(setup.getConfig().get());
        }
    }

    @Override
    public void afterTestSuccess() {
        shutdownCluster();
        temporaryFolder.delete();
    }

    @Override
    public void afterTestFailure() {
        if (distribution != null) {
            shutdownCluster();
            backupLogs();
        }
        temporaryFolder.delete();
    }

    private void shutdownCluster() {
        try {
            distribution.stopFlinkCluster();
        } catch (IOException e) {
            LOG.warn("Error while shutting down Flink cluster.", e);
        }
    }

    private void backupLogs() {
        if (logBackupDirectory != null) {
            final Path targetDirectory =
                    logBackupDirectory.resolve("flink-" + UUID.randomUUID().toString());
            try {
                distribution.copyLogsTo(targetDirectory);
                LOG.info("Backed up logs to {}.", targetDirectory);
            } catch (IOException e) {
                LOG.warn("An error has occurred while backing up logs to {}.", targetDirectory, e);
            }
        }
    }

    @Override
    public ClusterController startCluster(int numTaskManagers) throws IOException {
        distribution.setTaskExecutorHosts(Collections.nCopies(numTaskManagers, "localhost"));
        distribution.startFlinkCluster();

        try (final RestClient restClient =
                new RestClient(new Configuration(), Executors.directExecutor())) {
            for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
                final CompletableFuture<TaskManagersInfo> localhost =
                        restClient.sendRequest(
                                "localhost",
                                8081,
                                TaskManagersHeaders.getInstance(),
                                EmptyMessageParameters.getInstance(),
                                EmptyRequestBody.getInstance());

                try {
                    final TaskManagersInfo taskManagersInfo = localhost.get(1, TimeUnit.SECONDS);

                    final int numRunningTaskManagers =
                            taskManagersInfo.getTaskManagerInfos().size();
                    if (numRunningTaskManagers == numTaskManagers) {
                        return new StandaloneClusterController(distribution);
                    } else {
                        LOG.info(
                                "Waiting for task managers to come up. {}/{} are currently running.",
                                numRunningTaskManagers,
                                numTaskManagers);
                    }
                } catch (InterruptedException e) {
                    LOG.info("Waiting for dispatcher REST endpoint to come up...");
                    Thread.currentThread().interrupt();
                } catch (TimeoutException | ExecutionException e) {
                    // ExecutionExceptions may occur if leader election is still going on
                    LOG.info("Waiting for dispatcher REST endpoint to come up...");
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException("Could not create RestClient.", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException("Cluster did not start in expected time-frame.");
    }

    @Override
    public Stream<String> searchAllLogs(Pattern pattern, Function<Matcher, String> matchProcessor)
            throws IOException {
        return distribution.searchAllLogs(pattern, matchProcessor);
    }

    private static class StandaloneClusterController implements ClusterController {

        private final FlinkDistribution distribution;

        StandaloneClusterController(FlinkDistribution distribution) {
            this.distribution = distribution;
        }

        @Override
        public JobController submitJob(JobSubmission job, Duration timeout) throws IOException {
            final JobID run = distribution.submitJob(job, timeout);

            return new StandaloneJobController(run);
        }

        @Override
        public void submitSQLJob(SQLJobSubmission job, Duration timeout) throws IOException {
            distribution.submitSQLJob(job, timeout);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            try {
                distribution.stopFlinkCluster();
                return CompletableFuture.completedFuture(null);
            } catch (IOException e) {
                return FutureUtils.completedExceptionally(e);
            }
        }
    }

    private static class StandaloneJobController implements JobController {
        private final JobID jobId;

        StandaloneJobController(JobID jobId) {
            this.jobId = jobId;
        }
    }
}
