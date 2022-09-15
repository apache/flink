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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
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

import org.apache.flink.shaded.guava30.com.google.common.collect.MapDifference;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when {@link
 * StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

    /**
     * Due to the complexity of the current configuration stack, we need to limit the available
     * wildcard options for {@link DeploymentOptions#PROGRAM_CONFIG_WILDCARDS}.
     *
     * <p>If everything was backed by {@link Configuration} instead of the POJOs that partially
     * materialize the config options, the implementation could be way easier. Currently, we need to
     * manually provide the backward path from POJO to {@link ConfigOption} value here to let {@link
     * #collectNotAllowedConfigurations()} filter out wildcards in both POJOs and {@link
     * #configuration}.
     */
    private static final Map<String, WildcardOption<?>> SUPPORTED_PROGRAM_CONFIG_WILDCARDS =
            new HashMap<>();

    static {
        SUPPORTED_PROGRAM_CONFIG_WILDCARDS.put(
                PipelineOptions.GLOBAL_JOB_PARAMETERS.key(),
                new WildcardOption<>(
                        PipelineOptions.GLOBAL_JOB_PARAMETERS,
                        env -> env.getConfig().getGlobalJobParameters().toMap()));
    }

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
            }

            jobExecutionResult = jobExecutionResultFuture.get();
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
        final Configuration envConfigMap = new Configuration(configuration);

        // Removal must happen on Configuration objects (not instances of Map)
        // to also ignore map-typed config options with prefix key notation
        removeProgramConfigWildcards(clusterConfigMap);
        removeProgramConfigWildcards(envConfigMap);

        // Check Configuration
        final MapDifference<String, String> diff =
                Maps.difference(clusterConfigMap.toMap(), envConfigMap.toMap());
        diff.entriesOnlyOnRight()
                .forEach(
                        (k, v) ->
                                errors.add(
                                        ConfigurationNotAllowedMessage.ofConfigurationKeyAndValue(
                                                k, v)));
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
                                        ConfigurationNotAllowedMessage.ofConfigurationChange(
                                                k, v)));

        final Configuration enrichedClusterConfig = new Configuration(clusterConfiguration);
        enrichProgramConfigWildcards(enrichedClusterConfig);

        // Check CheckpointConfig
        final CheckpointConfig clusterCheckpointConfig = new CheckpointConfig();
        clusterCheckpointConfig.configure(enrichedClusterConfig);
        if (!Arrays.equals(
                serializeConfig(clusterCheckpointConfig), serializeConfig(checkpointCfg))) {
            errors.add(
                    ConfigurationNotAllowedMessage.ofConfigurationObject(
                            checkpointCfg.getClass().getSimpleName()));
        }

        // Check ExecutionConfig
        final ExecutionConfig clusterExecutionConfig = new ExecutionConfig();
        clusterExecutionConfig.configure(enrichedClusterConfig, this.getUserClassloader());
        if (!Arrays.equals(serializeConfig(clusterExecutionConfig), serializeConfig(config))) {
            errors.add(
                    ConfigurationNotAllowedMessage.ofConfigurationObject(
                            config.getClass().getSimpleName()));
        }

        return errors;
    }

    private void enrichProgramConfigWildcards(Configuration mutableConfig) {
        for (String key : programConfigWildcards) {
            final WildcardOption<?> option = SUPPORTED_PROGRAM_CONFIG_WILDCARDS.get(key);
            if (option == null) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Unsupported option '%s' for program configuration wildcards.",
                                key));
            }
            option.enrich(mutableConfig, this);
        }
    }

    private void removeProgramConfigWildcards(Configuration mutableConfig) {
        for (String key : programConfigWildcards) {
            final WildcardOption<?> option = SUPPORTED_PROGRAM_CONFIG_WILDCARDS.get(key);
            if (option == null) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Unsupported option '%s' for program configuration wildcards.",
                                key));
            }
            option.remove(mutableConfig);
        }
    }

    private static byte[] serializeConfig(Serializable config) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(config);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Cannot serialize configuration.", e);
        }
    }

    /**
     * Helper class for mapping a configuration key to a {@link ConfigOption} and a programmatic
     * getter.
     */
    private static final class WildcardOption<T> {
        private final ConfigOption<T> option;
        private final Function<StreamContextEnvironment, T> getter;

        WildcardOption(ConfigOption<T> option, Function<StreamContextEnvironment, T> getter) {
            this.option = option;
            this.getter = getter;
        }

        void enrich(Configuration mutableConfig, StreamContextEnvironment fromEnv) {
            mutableConfig.set(option, getter.apply(fromEnv));
        }

        void remove(Configuration mutableConfig) {
            mutableConfig.removeConfig(option);
        }
    }
}
