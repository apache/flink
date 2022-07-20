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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.ConfigurationNotAllowedMessage;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private final byte[] originalCheckpointConfigSerialized;
    private final byte[] originalExecutionConfigSerialized;
    private final Configuration originalConfiguration;

    private int jobCounter;

    private final Collection<String> errorMessages;

    private final boolean allowConfigurations;

    public StreamContextEnvironment(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration configuration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout) {
        this(
                executorServiceLoader,
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
            final Configuration configuration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout,
            final boolean allowConfigurations,
            final Collection<String> errorMessages) {
        super(executorServiceLoader, configuration, userCodeClassLoader);
        this.suppressSysout = suppressSysout;
        this.enforceSingleJobExecution = enforceSingleJobExecution;
        this.allowConfigurations = allowConfigurations;
        this.originalCheckpointConfigSerialized = serializeConfig(checkpointCfg);
        this.originalExecutionConfigSerialized = serializeConfig(config);
        this.originalConfiguration = new Configuration(configuration);
        this.errorMessages = errorMessages;
        this.jobCounter = 0;
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

    private void checkNotAllowedConfigurations() throws MutatedConfigurationException {
        errorMessages.addAll(collectNotAllowedConfigurations());
        if (!errorMessages.isEmpty()) {
            throw new MutatedConfigurationException(errorMessages);
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
            final Configuration configuration,
            final ClassLoader userCodeClassLoader,
            final boolean enforceSingleJobExecution,
            final boolean suppressSysout) {
        StreamExecutionEnvironmentFactory factory =
                conf -> {
                    final List<String> errors = new ArrayList<>();
                    final boolean allowConfigurations =
                            configuration.getBoolean(
                                    DeploymentOptions.ALLOW_CLIENT_JOB_CONFIGURATIONS);
                    if (!allowConfigurations && !conf.toMap().isEmpty()) {
                        conf.toMap()
                                .forEach(
                                        (k, v) ->
                                                errors.add(
                                                        ConfigurationNotAllowedMessage
                                                                .ofConfigurationKeyAndValue(k, v)));
                    }
                    Configuration mergedConfiguration = new Configuration();
                    mergedConfiguration.addAll(configuration);
                    mergedConfiguration.addAll(conf);
                    return new StreamContextEnvironment(
                            executorServiceLoader,
                            mergedConfiguration,
                            userCodeClassLoader,
                            enforceSingleJobExecution,
                            suppressSysout,
                            allowConfigurations,
                            errors);
                };
        initializeContextEnvironment(factory);
    }

    public static void unsetAsContext() {
        resetContextEnvironment();
    }

    private List<String> collectNotAllowedConfigurations() {
        final List<String> errors = new ArrayList<>();
        if (allowConfigurations) {
            return errors;
        }
        final MapDifference<String, String> diff =
                Maps.difference(originalConfiguration.toMap(), configuration.toMap());
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

        if (!Arrays.equals(originalCheckpointConfigSerialized, serializeConfig(checkpointCfg))) {
            errors.add(
                    ConfigurationNotAllowedMessage.ofConfigurationObject(
                            checkpointCfg.getClass().getSimpleName()));
        }

        if (!Arrays.equals(originalExecutionConfigSerialized, serializeConfig(config))) {
            errors.add(
                    ConfigurationNotAllowedMessage.ofConfigurationObject(
                            config.getClass().getSimpleName()));
        }
        return errors;
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
}
