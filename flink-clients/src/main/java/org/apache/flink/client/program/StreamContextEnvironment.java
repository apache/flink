/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.ConfigurationNotAllowedMessage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.MapDifference;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when {@link
 * StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

    private final boolean suppressSysout;

    private final boolean enforceSingleJobExecution;
    private final Configuration clusterConfiguration;

    private int jobCounter;

    private final boolean programConfigEnabled;
    private final Collection<String> programConfigWildcards;

    public StreamContextEnvironment(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration configuration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout) {
        this(
                executorServiceLoader,
                configuration,
                configuration,
                userCodeClassLoader,
                enforceSingleJobExecution,
                suppressSysout,
                true,
                Collections.emptyList());
    }

    @Internal
    public StreamContextEnvironment(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration clusterConfiguration,
            final Configuration configuration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout,
            final boolean programConfigEnabled,
            final Collection<String> programConfigWildcards) {
        super(executorServiceLoader, configuration, userCodeClassLoader);
        this.suppressSysout = suppressSysout;
        this.enforceSingleJobExecution = enforceSingleJobExecution;
        this.clusterConfiguration = clusterConfiguration;
        this.jobCounter = 0;
        this.programConfigEnabled = programConfigEnabled;
        this.programConfigWildcards = programConfigWildcards;
    }

    @Override
    public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
        final JobClient jobClient = executeAsync(streamGraph);
        final List<JobListener> jobListeners = getJobListeners();

        try {
            final JobExecutionResult jobExecutionResult = getJobExecutionResult(jobClient);
            jobListeners.forEach(
                    jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
            return jobExecutionResult;
        } catch (Throwable t) {
            jobListeners.forEach(
                    jobListener ->
                            jobListener.onJobExecuted(
                                    null, ExceptionUtils.stripExecutionException(t)));
            ExceptionUtils.rethrowException(t);

            // never reached, only make javac happy
            return null;
        }
    }

    private JobExecutionResult getJobExecutionResult(final JobClient jobClient) throws Exception {
        checkNotNull(jobClient);

        JobExecutionResult jobExecutionResult;
        if (configuration.getBoolean(DeploymentOptions.ATTACHED)) {
            CompletableFuture<JobExecutionResult> jobExecutionResultFuture =
                    jobClient.getJobExecutionResult();

            ScheduledExecutorService clientHeartbeatService = null;
            if (configuration.getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
                Thread shutdownHook =
                        ShutdownHookUtil.addShutdownHook(
                                () -> {
                                    // wait a smidgen to allow the async request to go through
                                    // before
                                    // the jvm exits
                                    jobClient.cancel().get(1, TimeUnit.SECONDS);
                                },
                                StreamContextEnvironment.class.getSimpleName(),
                                LOG);
                jobExecutionResultFuture.whenComplete(
                        (ignored, throwable) ->
                                ShutdownHookUtil.removeShutdownHook(
                                        shutdownHook,
                                        StreamContextEnvironment.class.getSimpleName(),
                                        LOG));
                clientHeartbeatService =
                        ClientUtils.reportHeartbeatPeriodically(
                                jobClient,
                                configuration.getLong(ClientOptions.CLIENT_HEARTBEAT_INTERVAL),
                                configuration.getLong(ClientOptions.CLIENT_HEARTBEAT_TIMEOUT));
            }

            jobExecutionResult = jobExecutionResultFuture.get();
            if (clientHeartbeatService != null) {
                clientHeartbeatService.shutdown();
            }
            if (!suppressSysout) {
                System.out.println(jobExecutionResult);
            }
        } else {
            jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
        }

        return jobExecutionResult;
    }

    @Override
    public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
        checkNotAllowedConfigurations();
        validateAllowedExecution();
        final JobClient jobClient = super.executeAsync(streamGraph);

        if (!suppressSysout) {
            System.out.println("Job has been submitted with JobID " + jobClient.getJobID());
        }

        return jobClient;
    }

    private void validateAllowedExecution() {
        if (enforceSingleJobExecution && jobCounter > 0) {
            throw new FlinkRuntimeException(
                    "Cannot have more than one execute() or executeAsync() call in a single environment.");
        }
        jobCounter++;
    }

    // --------------------------------------------------------------------------------------------

    public static void setAsContext(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration clusterConfiguration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout) {
        final StreamExecutionEnvironmentFactory factory =
                envInitConfig -> {
                    final boolean programConfigEnabled =
                            clusterConfiguration.get(DeploymentOptions.PROGRAM_CONFIG_ENABLED);
                    final List<String> programConfigWildcards =
                            clusterConfiguration.get(DeploymentOptions.PROGRAM_CONFIG_WILDCARDS);
                    final Configuration mergedEnvConfig = new Configuration();
                    mergedEnvConfig.addAll(clusterConfiguration);
                    mergedEnvConfig.addAll(envInitConfig);
                    return new StreamContextEnvironment(
                            executorServiceLoader,
                            clusterConfiguration,
                            mergedEnvConfig,
                            userCodeClassLoader,
                            enforceSingleJobExecution,
                            suppressSysout,
                            programConfigEnabled,
                            programConfigWildcards);
                };
        initializeContextEnvironment(factory);
    }

    public static void unsetAsContext() {
        resetContextEnvironment();
    }

    // --------------------------------------------------------------------------------------------
    // Program Configuration Validation
    // --------------------------------------------------------------------------------------------

    private void checkNotAllowedConfigurations() throws MutatedConfigurationException {
        final Collection<String> errorMessages = collectNotAllowedConfigurations();
        if (!errorMessages.isEmpty()) {
            throw new MutatedConfigurationException(errorMessages);
        }
    }

    /**
     * Collects programmatic configuration changes.
     *
     * <p>Configuration is spread across instances of {@link Configuration} and POJOs (e.g. {@link
     * ExecutionConfig}), so we need to have logic for comparing both. For supporting wildcards, the
     * first can be accomplished by simply removing keys, the latter by setting equal fields before
     * comparison.
     */
    private Collection<String> collectNotAllowedConfigurations() {
        if (programConfigEnabled) {
            return Collections.emptyList();
        }

        final List<String> errors = new ArrayList<>();

        final Configuration clusterConfigMap = new Configuration(clusterConfiguration);

        // Removal must happen on Configuration objects (not instances of Map)
        // to also ignore map-typed config options with prefix key notation
        removeProgramConfigWildcards(clusterConfigMap);

        checkMainConfiguration(clusterConfigMap, errors);
        checkCheckpointConfig(clusterConfigMap, errors);
        checkExecutionConfig(clusterConfigMap, errors);
        return errors;
    }

    private void checkMainConfiguration(Configuration clusterConfigMap, List<String> errors) {
        final Configuration envConfigMap = new Configuration(configuration);
        removeProgramConfigWildcards(envConfigMap);

        final MapDifference<String, String> diff =
                Maps.difference(clusterConfigMap.toMap(), envConfigMap.toMap());
        diff.entriesOnlyOnRight()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationAdded(k, v)));
        diff.entriesOnlyOnLeft()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationRemoved(
                                                k, v)));
        diff.entriesDiffering()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationChanged(
                                                k, v)));
    }

    private void checkCheckpointConfig(Configuration clusterConfigMap, List<String> errors) {
        CheckpointConfig expectedCheckpointConfig = new CheckpointConfig();
        expectedCheckpointConfig.configure(clusterConfigMap);
        configureCheckpointStorage(clusterConfigMap, expectedCheckpointConfig);
        checkConfigurationObject(
                expectedCheckpointConfig.toConfiguration(),
                checkpointCfg.toConfiguration(),
                checkpointCfg.getClass().getSimpleName(),
                errors);

        /**
         * Unfortunately, {@link CheckpointConfig#setCheckpointStorage} is not backed by a {@link
         * Configuration}, but it also has to be validated. For this validation we are implementing
         * a one off manual check.
         */
        if (!programConfigWildcards.contains(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key())
                && !Objects.equals(
                        checkpointCfg.getCheckpointStorage(),
                        expectedCheckpointConfig.getCheckpointStorage())) {
            errors.add(
                    ConfigurationNotAllowedMessage.ofConfigurationObjectSetterUsed(
                            checkpointCfg.getClass().getSimpleName(), "setCheckpointStorage"));
        }
    }

    private void checkExecutionConfig(Configuration clusterConfigMap, List<String> errors) {
        ExecutionConfig expectedExecutionConfig = new ExecutionConfig();
        expectedExecutionConfig.configure(clusterConfigMap, getUserClassloader());
        checkConfigurationObject(
                expectedExecutionConfig.toConfiguration(),
                config.toConfiguration(),
                config.getClass().getSimpleName(),
                errors);
    }

    private void checkConfigurationObject(
            Configuration expectedConfiguration,
            Configuration actualConfiguration,
            String configurationObjectName,
            List<String> errors) {
        removeProgramConfigWildcards(actualConfiguration);

        final MapDifference<String, String> diff =
                Maps.difference(expectedConfiguration.toMap(), actualConfiguration.toMap());
        diff.entriesOnlyOnRight()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationObjectAdded(
                                                configurationObjectName, k, v)));
        diff.entriesDiffering()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationObjectChanged(
                                                configurationObjectName, k, v)));

        diff.entriesOnlyOnLeft()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationObjectRemoved(
                                                configurationObjectName, k, v)));
    }

    private void removeProgramConfigWildcards(Configuration mutableConfig) {
        for (String key : programConfigWildcards) {
            mutableConfig.removeKey(key);
        }
    }
}
