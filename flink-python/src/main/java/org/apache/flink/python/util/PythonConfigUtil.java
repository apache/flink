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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.PythonPartitionCustomOperator;
import org.apache.flink.streaming.api.operators.python.StatelessOneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.StatelessTwoInputPythonFunctionOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * A Util class to get the {@link StreamExecutionEnvironment} configuration and merged configuration with environment
 * settings.
 */
public class PythonConfigUtil {

	public static final String KEYED_STREAM_VALUE_OPERATOR_NAME = "_keyed_stream_values_operator";
	public static final String STREAM_KEY_BY_MAP_OPERATOR_NAME = "_stream_key_by_map_operator";
	public static final String STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME = "_partition_custom_map_operator";

	/**
	 * A static method to get the {@link StreamExecutionEnvironment} configuration merged with python dependency
	 * management configurations.
	 */
	public static Configuration getEnvConfigWithDependencies(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Configuration envConfiguration = getEnvironmentConfig(env);
		Configuration config = PythonDependencyUtils.configurePythonDependencies(env.getCachedFiles(),
			envConfiguration);
		return config;
	}

	/**
	 * Get the private method {@link StreamExecutionEnvironment#getConfiguration()} by reflection recursively. Then
	 * access the method to get the configuration of the given StreamExecutionEnvironment.
	 */
	public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Method getConfigurationMethod = null;
		for (Class<?> clz = env.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
			try {
				getConfigurationMethod = clz.getDeclaredMethod("getConfiguration");
				break;
			} catch (NoSuchMethodException e) {

			}
		}

		if (getConfigurationMethod == null) {
			throw new NoSuchMethodException("Method getConfigurationMethod not found.");
		}

		getConfigurationMethod.setAccessible(true);
		Configuration envConfiguration = (Configuration) getConfigurationMethod.invoke(env);
		return envConfiguration;
	}

	/**
	 * Configure the {@link StatelessOneInputPythonFunctionOperator} to be chained with the upstream/downstream
	 * operator by setting their parallelism, slot sharing group, co-location group to be the same, and applying a
	 * {@link ForwardPartitioner}.
	 * 1. operator with name "_keyed_stream_values_operator" should align with its downstream operator.
	 * 2. operator with name "_stream_key_by_map_operator" should align with its upstream operator.
	 */
	private static void alignStreamNode(StreamNode streamNode, StreamGraph streamGraph) {
		if (streamNode.getOperatorName().equals(KEYED_STREAM_VALUE_OPERATOR_NAME)) {
			StreamEdge downStreamEdge = streamNode.getOutEdges().get(0);
			StreamNode downStreamNode = streamGraph.getStreamNode(downStreamEdge.getTargetId());
			chainStreamNode(downStreamEdge, streamNode, downStreamNode);
			downStreamEdge.setPartitioner(new ForwardPartitioner());
		}

		if (streamNode.getOperatorName().equals(STREAM_KEY_BY_MAP_OPERATOR_NAME) ||
		streamNode.getOperatorName().equals(STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)) {
			StreamEdge upStreamEdge = streamNode.getInEdges().get(0);
			StreamNode upStreamNode = streamGraph.getStreamNode(upStreamEdge.getSourceId());
			chainStreamNode(upStreamEdge, streamNode, upStreamNode);
		}
	}

	private static void chainStreamNode(StreamEdge streamEdge, StreamNode firstStream, StreamNode secondStream){
		streamEdge.setPartitioner(new ForwardPartitioner<>());
		firstStream.setParallelism(secondStream.getParallelism());
		firstStream.setCoLocationGroup(secondStream.getCoLocationGroup());
		firstStream.setSlotSharingGroup(secondStream.getSlotSharingGroup());
	}

	/**
	 * Generate a {@link StreamGraph} for transformations maintained by current {@link StreamExecutionEnvironment}, and
	 * reset the merged env configurations with dependencies to every {@link StatelessOneInputPythonFunctionOperator}.
	 * It is an idempotent operation that can be call multiple times. Remember that only when need to execute the
	 * StreamGraph can we set the clearTransformations to be True.
	 */
	public static StreamGraph generateStreamGraphWithDependencies(
		StreamExecutionEnvironment env, boolean clearTransformations) throws IllegalAccessException,
		NoSuchMethodException, InvocationTargetException {

		Configuration mergedConfig = getEnvConfigWithDependencies(env);
		StreamGraph streamGraph = env.getStreamGraph(StreamExecutionEnvironment.DEFAULT_JOB_NAME, clearTransformations);
		Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
		for (StreamNode streamNode : streamNodes) {

			alignStreamNode(streamNode, streamGraph);

			StreamOperatorFactory streamOperatorFactory = streamNode.getOperatorFactory();
			if (streamOperatorFactory instanceof SimpleOperatorFactory) {
				StreamOperator streamOperator = ((SimpleOperatorFactory) streamOperatorFactory).getOperator();
				if ((streamOperator instanceof StatelessOneInputPythonFunctionOperator) ||
					(streamOperator instanceof StatelessTwoInputPythonFunctionOperator)) {
					AbstractPythonFunctionOperator abstractPythonFunctionOperator =
						(AbstractPythonFunctionOperator) streamOperator;

					Configuration oldConfig = abstractPythonFunctionOperator.getPythonConfig()
						.getMergedConfig();
					abstractPythonFunctionOperator.setPythonConfig(generateNewPythonConfig(oldConfig,
						mergedConfig));
				}
			}
		}

		setStreamPartitionCustomOperatorNumPartitions(streamNodes, streamGraph);

		return streamGraph;
	}

	private static void setStreamPartitionCustomOperatorNumPartitions(
		Collection<StreamNode> streamNodes, StreamGraph streamGraph){
		for (StreamNode streamNode : streamNodes) {
			StreamOperatorFactory streamOperatorFactory = streamNode.getOperatorFactory();
			if (streamOperatorFactory instanceof SimpleOperatorFactory) {
				StreamOperator streamOperator = ((SimpleOperatorFactory) streamOperatorFactory).getOperator();
				if (streamOperator instanceof PythonPartitionCustomOperator) {
					PythonPartitionCustomOperator paritionCustomFunctionOperator =
						(PythonPartitionCustomOperator) streamOperator;

					// Update the numPartitions of PartitionCustomOperator after aligned all operators.
					paritionCustomFunctionOperator.setNumPartitions(
						streamGraph.getStreamNode(streamNode.getOutEdges().get(0).getTargetId()).getParallelism());
				}
			}
		}
	}

	/**
	 * Generator a new {@link  PythonConfig} with the combined config which is derived from oldConfig.
	 */
	private static PythonConfig generateNewPythonConfig(Configuration oldConfig, Configuration newConfig) {
		Configuration mergedConfig = newConfig.clone();
		mergedConfig.addAll(oldConfig);
		return new PythonConfig(mergedConfig);
	}

}
