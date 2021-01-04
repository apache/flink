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

package org.apache.flink.python.util;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.OneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.PythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.PythonPartitionCustomOperator;
import org.apache.flink.streaming.api.operators.python.PythonTimestampsAndWatermarksOperator;
import org.apache.flink.streaming.api.operators.python.TwoInputPythonFunctionOperator;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * A Util class to get the {@link StreamExecutionEnvironment} configuration and merged configuration
 * with environment settings.
 */
public class PythonConfigUtil {

    public static final String KEYED_STREAM_VALUE_OPERATOR_NAME = "_keyed_stream_values_operator";
    public static final String STREAM_KEY_BY_MAP_OPERATOR_NAME = "_stream_key_by_map_operator";
    public static final String STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME =
            "_partition_custom_map_operator";

    /**
     * A static method to get the {@link StreamExecutionEnvironment} configuration merged with
     * python dependency management configurations.
     */
    public static Configuration getEnvConfigWithDependencies(StreamExecutionEnvironment env)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        return PythonDependencyUtils.configurePythonDependencies(
                env.getCachedFiles(), getEnvironmentConfig(env));
    }

    /**
     * Get the private method {@link StreamExecutionEnvironment#getConfiguration()} by reflection
     * recursively. Then access the method to get the configuration of the given
     * StreamExecutionEnvironment.
     */
    public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method getConfigurationMethod = null;
        for (Class<?> clz = env.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            try {
                getConfigurationMethod = clz.getDeclaredMethod("getConfiguration");
                break;
            } catch (NoSuchMethodException e) {
                // ignore
            }
        }

        if (getConfigurationMethod == null) {
            throw new NoSuchMethodException("Method getConfigurationMethod not found.");
        }

        getConfigurationMethod.setAccessible(true);
        return (Configuration) getConfigurationMethod.invoke(env);
    }

    /**
     * Configure the {@link OneInputPythonFunctionOperator} to be chained with the
     * upstream/downstream operator by setting their parallelism, slot sharing group, co-location
     * group to be the same, and applying a {@link ForwardPartitioner}. 1. operator with name
     * "_keyed_stream_values_operator" should align with its downstream operator. 2. operator with
     * name "_stream_key_by_map_operator" should align with its upstream operator.
     */
    private static void alignStreamNode(StreamNode streamNode, StreamGraph streamGraph) {
        if (streamNode.getOperatorName().equals(KEYED_STREAM_VALUE_OPERATOR_NAME)) {
            StreamEdge downStreamEdge = streamNode.getOutEdges().get(0);
            StreamNode downStreamNode = streamGraph.getStreamNode(downStreamEdge.getTargetId());
            chainStreamNode(downStreamEdge, streamNode, downStreamNode);
            downStreamEdge.setPartitioner(new ForwardPartitioner());
        }

        if (streamNode.getOperatorName().equals(STREAM_KEY_BY_MAP_OPERATOR_NAME)
                || streamNode.getOperatorName().equals(STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)) {
            StreamEdge upStreamEdge = streamNode.getInEdges().get(0);
            StreamNode upStreamNode = streamGraph.getStreamNode(upStreamEdge.getSourceId());
            chainStreamNode(upStreamEdge, streamNode, upStreamNode);
        }
    }

    private static void chainStreamNode(
            StreamEdge streamEdge, StreamNode firstStream, StreamNode secondStream) {
        streamEdge.setPartitioner(new ForwardPartitioner<>());
        firstStream.setParallelism(secondStream.getParallelism());
        firstStream.setCoLocationGroup(secondStream.getCoLocationGroup());
        firstStream.setSlotSharingGroup(secondStream.getSlotSharingGroup());
    }

    /**
     * Generate a {@link StreamGraph} for transformations maintained by current {@link
     * StreamExecutionEnvironment}, and reset the merged env configurations with dependencies to
     * every {@link OneInputPythonFunctionOperator}. It is an idempotent operation that can be call
     * multiple times. Remember that only when need to execute the StreamGraph can we set the
     * clearTransformations to be True.
     */
    public static StreamGraph generateStreamGraphWithDependencies(
            StreamExecutionEnvironment env, boolean clearTransformations)
            throws IllegalAccessException, NoSuchMethodException, InvocationTargetException,
                    NoSuchFieldException {
        Configuration mergedConfig = getEnvConfigWithDependencies(env);

        boolean executedInBatchMode = isExecuteInBatchMode(env, mergedConfig);
        if (executedInBatchMode) {
            throw new UnsupportedOperationException(
                    "Batch mode is still not supported in Python DataStream API.");
        }

        if (mergedConfig.getBoolean(PythonOptions.USE_MANAGED_MEMORY)) {
            Field transformationsField =
                    StreamExecutionEnvironment.class.getDeclaredField("transformations");
            transformationsField.setAccessible(true);
            for (Transformation transform :
                    (List<Transformation<?>>) transformationsField.get(env)) {
                if (transform instanceof OneInputTransformation
                        && isPythonOperator(
                                ((OneInputTransformation) transform).getOperatorFactory())) {
                    transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
                } else if (transform instanceof TwoInputTransformation
                        && isPythonOperator(
                                ((TwoInputTransformation) transform).getOperatorFactory())) {
                    transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
                } else if (transform instanceof AbstractMultipleInputTransformation
                        && isPythonOperator(
                                ((AbstractMultipleInputTransformation) transform)
                                        .getOperatorFactory())) {
                    transform.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
                }
            }
        }

        String jobName =
                getEnvironmentConfig(env)
                        .getString(
                                PipelineOptions.NAME, StreamExecutionEnvironment.DEFAULT_JOB_NAME);
        StreamGraph streamGraph = env.getStreamGraph(jobName, clearTransformations);
        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        for (StreamNode streamNode : streamNodes) {
            alignStreamNode(streamNode, streamGraph);
            StreamOperatorFactory streamOperatorFactory = streamNode.getOperatorFactory();
            if (streamOperatorFactory instanceof SimpleOperatorFactory) {
                StreamOperator streamOperator =
                        ((SimpleOperatorFactory) streamOperatorFactory).getOperator();
                if ((streamOperator instanceof OneInputPythonFunctionOperator)
                        || (streamOperator instanceof TwoInputPythonFunctionOperator)
                        || (streamOperator instanceof PythonKeyedProcessOperator)) {
                    AbstractPythonFunctionOperator pythonFunctionOperator =
                            (AbstractPythonFunctionOperator) streamOperator;

                    Configuration oldConfig =
                            pythonFunctionOperator.getPythonConfig().getMergedConfig();
                    pythonFunctionOperator.setPythonConfig(
                            generateNewPythonConfig(oldConfig, mergedConfig));

                    if (streamOperator instanceof PythonTimestampsAndWatermarksOperator) {
                        ((PythonTimestampsAndWatermarksOperator) streamOperator)
                                .configureEmitProgressiveWatermarks(!executedInBatchMode);
                    }
                }
            }
        }

        setStreamPartitionCustomOperatorNumPartitions(streamNodes, streamGraph);

        return streamGraph;
    }

    private static boolean isPythonOperator(StreamOperatorFactory streamOperatorFactory) {
        if (streamOperatorFactory instanceof SimpleOperatorFactory) {
            return ((SimpleOperatorFactory) streamOperatorFactory).getOperator()
                    instanceof AbstractPythonFunctionOperator;
        } else {
            return false;
        }
    }

    private static void setStreamPartitionCustomOperatorNumPartitions(
            Collection<StreamNode> streamNodes, StreamGraph streamGraph) {
        for (StreamNode streamNode : streamNodes) {
            StreamOperatorFactory streamOperatorFactory = streamNode.getOperatorFactory();
            if (streamOperatorFactory instanceof SimpleOperatorFactory) {
                StreamOperator streamOperator =
                        ((SimpleOperatorFactory) streamOperatorFactory).getOperator();
                if (streamOperator instanceof PythonPartitionCustomOperator) {
                    PythonPartitionCustomOperator partitionCustomFunctionOperator =
                            (PythonPartitionCustomOperator) streamOperator;
                    // Update the numPartitions of PartitionCustomOperator after aligned all
                    // operators.
                    partitionCustomFunctionOperator.setNumPartitions(
                            streamGraph
                                    .getStreamNode(streamNode.getOutEdges().get(0).getTargetId())
                                    .getParallelism());
                }
            }
        }
    }

    /**
     * Generator a new {@link PythonConfig} with the combined config which is derived from
     * oldConfig.
     */
    private static PythonConfig generateNewPythonConfig(
            Configuration oldConfig, Configuration newConfig) {
        Configuration mergedConfig = newConfig.clone();
        mergedConfig.addAll(oldConfig);
        return new PythonConfig(mergedConfig);
    }

    /** Return is executed in batch mode according to the configured RuntimeExecutionMode. */
    private static boolean isExecuteInBatchMode(
            StreamExecutionEnvironment env, Configuration configuration)
            throws NoSuchFieldException, IllegalAccessException {

        final RuntimeExecutionMode executionMode = configuration.get(ExecutionOptions.RUNTIME_MODE);
        if (executionMode != RuntimeExecutionMode.AUTOMATIC) {
            return executionMode == RuntimeExecutionMode.BATCH;
        }

        Field transformationsField =
                StreamExecutionEnvironment.class.getDeclaredField("transformations");
        transformationsField.setAccessible(true);
        boolean existsUnboundedSource = false;
        for (Transformation transform : (List<Transformation<?>>) transformationsField.get(env)) {
            existsUnboundedSource =
                    existsUnboundedSource
                            || (transform instanceof WithBoundedness
                                    && ((WithBoundedness) transform).getBoundedness()
                                            != Boundedness.BOUNDED);
        }
        return !existsUnboundedSource;
    }
}
