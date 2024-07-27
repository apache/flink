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

package org.apache.flink.datastream.impl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.FromDataSource;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.runtime.translators.DataStreamV2SinkTransformationTranslator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.api.graph.StreamGraphGenerator.DEFAULT_TIME_CHARACTERISTIC;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link ExecutionEnvironment}.
 *
 * <p>IMPORTANT: Even though this is not part of public API, {@link ExecutionEnvironment} will get
 * this class instance through reflection, so we must ensure that the package path, class name and
 * the signature of {@link #newInstance()} does not change.
 */
public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final List<Transformation<?>> transformations = new ArrayList<>();

    private final ExecutionConfig executionConfig;

    /** Settings that control the checkpointing behavior. */
    private final CheckpointConfig checkpointCfg;

    private final Configuration configuration;

    private final ClassLoader userClassloader;

    private final PipelineExecutorServiceLoader executorServiceLoader;

    /**
     * The environment of the context (local by default, cluster if invoked through command line).
     */
    private static ExecutionEnvironmentFactory contextEnvironmentFactory = null;

    static {
        try {
            // All transformation translator must be put to a map in StreamGraphGenerator, but
            // streaming-java is not depend on process-function module, using reflect to handle
            // this.
            DataStreamV2SinkTransformationTranslator.registerSinkTransformationTranslator();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Can not register process function transformation translator.", e);
        }
    }

    /**
     * Create and return an instance of {@link ExecutionEnvironment}.
     *
     * <p>IMPORTANT: The method is only expected to be called by {@link ExecutionEnvironment} via
     * reflection, so we must ensure that the package path, class name and the signature of this
     * method does not change.
     */
    public static ExecutionEnvironment newInstance() {
        if (contextEnvironmentFactory != null) {
            return contextEnvironmentFactory.createExecutionEnvironment(new Configuration());
        } else {
            final Configuration configuration = new Configuration();
            configuration.set(DeploymentOptions.TARGET, "local");
            configuration.set(DeploymentOptions.ATTACHED, true);
            return new ExecutionEnvironmentImpl(
                    new DefaultExecutorServiceLoader(), configuration, null);
        }
    }

    ExecutionEnvironmentImpl(
            PipelineExecutorServiceLoader executorServiceLoader,
            Configuration configuration,
            ClassLoader classLoader) {
        this.executorServiceLoader = checkNotNull(executorServiceLoader);
        this.configuration = configuration;
        this.executionConfig = new ExecutionConfig(this.configuration);
        this.checkpointCfg = new CheckpointConfig(this.configuration);
        this.userClassloader = classLoader == null ? getClass().getClassLoader() : classLoader;
        configure(configuration, userClassloader);
    }

    @Override
    public void execute(String jobName) throws Exception {
        StreamGraph streamGraph = getStreamGraph();
        if (jobName != null) {
            streamGraph.setJobName(jobName);
        }

        execute(streamGraph);
    }

    @Override
    public RuntimeExecutionMode getExecutionMode() {
        return configuration.get(ExecutionOptions.RUNTIME_MODE);
    }

    @Override
    public ExecutionEnvironment setExecutionMode(RuntimeExecutionMode runtimeMode) {
        checkNotNull(runtimeMode);
        configuration.set(ExecutionOptions.RUNTIME_MODE, runtimeMode);
        return this;
    }

    protected static void initializeContextEnvironment(ExecutionEnvironmentFactory ctx) {
        contextEnvironmentFactory = ctx;
    }

    protected static void resetContextEnvironment() {
        contextEnvironmentFactory = null;
    }

    @Override
    public <OUT> NonKeyedPartitionStream<OUT> fromSource(Source<OUT> source, String sourceName) {
        if (source instanceof WrappedSource) {
            org.apache.flink.api.connector.source.Source<OUT, ?, ?> innerSource =
                    ((WrappedSource<OUT>) source).getWrappedSource();
            final TypeInformation<OUT> resolvedTypeInfo =
                    getSourceTypeInfo(innerSource, sourceName);

            SourceTransformation<OUT, ?, ?> sourceTransformation =
                    new SourceTransformation<>(
                            sourceName,
                            innerSource,
                            WatermarkStrategy.noWatermarks(),
                            resolvedTypeInfo,
                            getParallelism(),
                            false);
            return new NonKeyedPartitionStreamImpl<>(this, sourceTransformation);
        } else if (source instanceof FromDataSource) {
            Collection<OUT> data = ((FromDataSource<OUT>) source).getData();
            TypeInformation<OUT> outType = extractTypeInfoFromCollection(data);

            FromElementsGeneratorFunction<OUT> generatorFunction =
                    new FromElementsGeneratorFunction<>(outType, executionConfig, data);

            DataGeneratorSource<OUT> generatorSource =
                    new DataGeneratorSource<>(generatorFunction, data.size(), outType);

            return fromSource(new WrappedSource<>(generatorSource), "Collection Source");
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported type of sink, you could use DataStreamV2SourceUtils to wrap a FLIP-27 based source.");
        }
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public int getParallelism() {
        return executionConfig.getParallelism();
    }

    public List<Transformation<?>> getTransformations() {
        return transformations;
    }

    public void setParallelism(int parallelism) {
        executionConfig.setParallelism(parallelism);
    }

    public CheckpointConfig getCheckpointCfg() {
        return checkpointCfg;
    }

    // -----------------------------------------------
    //              Internal Methods
    // -----------------------------------------------

    private static <OUT> TypeInformation<OUT> extractTypeInfoFromCollection(Collection<OUT> data) {
        Preconditions.checkNotNull(data, "Collection must not be null");
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Collection must not be empty");
        }

        OUT first = data.iterator().next();
        if (first == null) {
            throw new IllegalArgumentException("Collection must not contain null elements");
        }

        TypeInformation<OUT> typeInfo;
        try {
            typeInfo = TypeExtractor.getForObject(first);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not create TypeInformation for type "
                            + first.getClass()
                            + "; please specify the TypeInformation manually via the version of the "
                            + "method that explicitly accepts it as an argument.",
                    e);
        }
        return typeInfo;
    }

    @SuppressWarnings("unchecked")
    private static <OUT, T extends TypeInformation<OUT>> T getSourceTypeInfo(
            org.apache.flink.api.connector.source.Source<OUT, ?, ?> source, String sourceName) {
        TypeInformation<OUT> resolvedTypeInfo = null;
        if (source instanceof ResultTypeQueryable) {
            resolvedTypeInfo = ((ResultTypeQueryable<OUT>) source).getProducedType();
        }
        if (resolvedTypeInfo == null) {
            try {
                resolvedTypeInfo =
                        TypeExtractor.createTypeInfo(
                                org.apache.flink.api.connector.source.Source.class,
                                source.getClass(),
                                0,
                                null,
                                null);
            } catch (final InvalidTypesException e) {
                resolvedTypeInfo = (TypeInformation<OUT>) new MissingTypeInfo(sourceName, e);
            }
        }
        return (T) resolvedTypeInfo;
    }

    public void addOperator(Transformation<?> transformation) {
        checkNotNull(transformation, "transformation must not be null.");
        this.transformations.add(transformation);
    }

    private void execute(StreamGraph streamGraph) throws Exception {
        final JobClient jobClient = executeAsync(streamGraph);

        try {
            if (configuration.get(DeploymentOptions.ATTACHED)) {
                jobClient.getJobExecutionResult().get();
            }
            // TODO Supports accumulator.
        } catch (Throwable t) {
            // get() on the JobExecutionResult Future will throw an ExecutionException. This
            // behaviour was largely not there in Flink versions before the PipelineExecutor
            // refactoring so we should strip that exception.
            Throwable strippedException = ExceptionUtils.stripExecutionException(t);
            ExceptionUtils.rethrowException(strippedException);
        }
    }

    private JobClient executeAsync(StreamGraph streamGraph) throws Exception {
        checkNotNull(streamGraph, "StreamGraph cannot be null.");
        final PipelineExecutor executor = getPipelineExecutor();

        CompletableFuture<JobClient> jobClientFuture =
                executor.execute(streamGraph, configuration, getClass().getClassLoader());

        try {
            // TODO Supports job listeners.
            return jobClientFuture.get();
        } catch (ExecutionException executionException) {
            final Throwable strippedException =
                    ExceptionUtils.stripExecutionException(executionException);
            throw new FlinkException(
                    String.format("Failed to execute job '%s'.", streamGraph.getJobName()),
                    strippedException);
        }
    }

    /** Get {@link StreamGraph} and clear all transformations. */
    public StreamGraph getStreamGraph() {
        final StreamGraph streamGraph = getStreamGraphGenerator(transformations).generate();
        transformations.clear();
        return streamGraph;
    }

    private StreamGraphGenerator getStreamGraphGenerator(List<Transformation<?>> transformations) {
        if (transformations.size() <= 0) {
            throw new IllegalStateException(
                    "No operators defined in streaming topology. Cannot execute.");
        }

        // We copy the transformation so that newly added transformations cannot intervene with the
        // stream graph generation.
        return new StreamGraphGenerator(
                        new ArrayList<>(transformations),
                        executionConfig,
                        checkpointCfg,
                        configuration)
                .setTimeCharacteristic(DEFAULT_TIME_CHARACTERISTIC);
    }

    private PipelineExecutor getPipelineExecutor() throws Exception {
        checkNotNull(
                configuration.get(DeploymentOptions.TARGET),
                "No execution.target specified in your configuration file.");

        final PipelineExecutorFactory executorFactory =
                executorServiceLoader.getExecutorFactory(configuration);

        checkNotNull(
                executorFactory,
                "Cannot find compatible factory for specified execution.target (=%s)",
                configuration.get(DeploymentOptions.TARGET));

        return executorFactory.getExecutor(configuration);
    }

    private void configure(ReadableConfig configuration, ClassLoader classLoader) {
        this.configuration.addAll(Configuration.fromMap(configuration.toMap()));
        executionConfig.configure(configuration, classLoader);
        checkpointCfg.configure(configuration);
    }
}
