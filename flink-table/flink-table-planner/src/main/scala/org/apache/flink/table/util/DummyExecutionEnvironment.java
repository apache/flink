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

package org.apache.flink.table.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;

/**
 * This is dummy {@link ExecutionEnvironment}, which holds a real {@link ExecutionEnvironment},
 * shares all configurations of the real environment, and disables all configuration setting methods.
 *
 * <p>When translating relational plan to execution plan in the {@link BatchTableEnvironment},
 * the generated {@link DataSink}s will be added into ExecutionEnvironment's sink buffer, and they will be cleared
 * only when {@link ExecutionEnvironment#execute()} method is called. Each {@link BatchTableEnvironment} instance holds
 * an immutable ExecutionEnvironment instance. If there are multiple translations (not all for `execute`,
 * e.g. `explain` and then `execute`) in one BatchTableEnvironment instance, the sink buffer is dirty,
 * and execution result may be incorrect.
 *
 * <p>This dummy ExecutionEnvironment is only used for buffering the sinks generated in ExecutionEnvironment.
 * A new dummy ExecutionEnvironment instance should be created for each translation, and this could avoid
 * dirty the buffer of the real ExecutionEnvironment instance.
 *
 * <p>All set methods (e.g. `setXX`, `enableXX`, `disableXX`, etc) are disabled to prohibit changing configuration,
 * all get methods (e.g. `getXX`, `isXX`, etc) will be delegated to the real ExecutionEnvironment.
 * `execute`, `executeAsync` methods are also disabled, while `registerDataSink`  method is enabled to
 * allow the BatchTableEnvironment to add DataSink to the dummy ExecutionEnvironment.
 *
 * <p>NOTE: Please remove {@code com.esotericsoftware.kryo} item in the whitelist of checkCodeDependencies()
 * method in {@code test_table_shaded_dependencies.sh} end-to-end test when this class is removed.
 */
public class DummyExecutionEnvironment extends ExecutionEnvironment {

	private final ExecutionEnvironment realExecEnv;

	public DummyExecutionEnvironment(ExecutionEnvironment realExecEnv) {
		super(realExecEnv.getExecutorServiceLoader(),
			realExecEnv.getConfiguration(),
			realExecEnv.getUserCodeClassLoader());
		this.realExecEnv = realExecEnv;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return realExecEnv.getUserCodeClassLoader();
	}

	@Override
	public PipelineExecutorServiceLoader getExecutorServiceLoader() {
		return realExecEnv.getExecutorServiceLoader();
	}

	@Override
	public Configuration getConfiguration() {
		return realExecEnv.getConfiguration();
	}

	@Override
	public ExecutionConfig getConfig() {
		return realExecEnv.getConfig();
	}

	@Override
	public int getParallelism() {
		return realExecEnv.getParallelism();
	}

	@Override
	public void setParallelism(int parallelism) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, setParallelism method is unsupported.");
	}

	@Override
	public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, setRestartStrategy method is unsupported.");
	}

	@Override
	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		return realExecEnv.getRestartStrategy();
	}

	@Override
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, setNumberOfExecutionRetries method is unsupported.");
	}

	@Override
	public int getNumberOfExecutionRetries() {
		return realExecEnv.getNumberOfExecutionRetries();
	}

	@Override
	public JobExecutionResult getLastJobExecutionResult() {
		return realExecEnv.getLastJobExecutionResult();
	}

	@Override
	public <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(Class<?> type, T serializer) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, addDefaultKryoSerializer method is unsupported.");
	}

	@Override
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, addDefaultKryoSerializer method is unsupported.");
	}

	@Override
	public <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerTypeWithKryoSerializer method is unsupported.");
	}

	@Override
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerTypeWithKryoSerializer method is unsupported.");
	}

	@Override
	public void registerType(Class<?> type) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerType method is unsupported.");
	}

	@Override
	public void configure(ReadableConfig configuration, ClassLoader classLoader) {
		//
	}

	@Override
	public void registerJobListener(JobListener jobListener) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerJobListener method is unsupported.");
	}

	@Override
	public void clearJobListeners() {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, clearJobListeners method is unsupported.");
	}

	@Override
	public void registerCachedFile(String filePath, String name) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerCachedFile method is unsupported.");
	}

	@Override
	public void registerCachedFile(String filePath, String name, boolean executable) {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, registerCachedFile method is unsupported.");
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, execute method is unsupported.");
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, execute method is unsupported.");
	}

	@Override
	public JobClient executeAsync(String jobName) throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy ExecutionEnvironment, executeAsync method is unsupported.");
	}
}
