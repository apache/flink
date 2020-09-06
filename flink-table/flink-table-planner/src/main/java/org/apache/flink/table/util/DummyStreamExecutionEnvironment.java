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
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.List;

/**
 * This is dummy {@link StreamExecutionEnvironment}, which holds a real {@link StreamExecutionEnvironment},
 * shares all configurations of the real environment, and disables all configuration setting methods.
 *
 * <p>When translating relational plan to execution plan in the {@link Planner}, the generated {@link Transformation}s
 * will be added into StreamExecutionEnvironment's buffer, and they will be cleared only when
 * {@link StreamExecutionEnvironment#execute()} method is called. Each {@link TableEnvironment} instance holds
 * an immutable StreamExecutionEnvironment instance. If there are multiple translations (not all for `execute`,
 * e.g. `explain` and then `execute`) in one TableEnvironment instance, the transformation buffer is dirty,
 * and execution result may be incorrect.
 *
 * <p>This dummy StreamExecutionEnvironment is only used for buffering the transformations generated in the planner.
 * A new dummy StreamExecutionEnvironment instance should be created for each translation, and this could avoid
 * dirty the buffer of the real StreamExecutionEnvironment instance.
 *
 * <p>All set methods (e.g. `setXX`, `enableXX`, `disableXX`, etc) are disabled to prohibit changing configuration,
 * all get methods (e.g. `getXX`, `isXX`, etc) will be delegated to the real StreamExecutionEnvironment.
 * `execute`, `getStreamGraph` methods are also disabled, while `addOperator` method is enabled to
 * allow the planner to add the generated transformations to the dummy StreamExecutionEnvironment.
 *
 * <p>This class could be removed once the {@link StreamTableSource} interface and {@link StreamTableSink} interface
 * are reworked.
 *
 * <p>NOTE: Please remove {@code com.esotericsoftware.kryo} item in the whitelist of checkCodeDependencies()
 * method in {@code test_table_shaded_dependencies.sh} end-to-end test when this class is removed.
 */
public class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

	private final StreamExecutionEnvironment realExecEnv;

	public DummyStreamExecutionEnvironment(StreamExecutionEnvironment realExecEnv) {
		this.realExecEnv = realExecEnv;
	}

	@Override
	public ExecutionConfig getConfig() {
		return realExecEnv.getConfig();
	}

	@Override
	public List<Tuple2<String, DistributedCache.DistributedCacheEntry>> getCachedFiles() {
		return realExecEnv.getCachedFiles();
	}

	@Override
	public StreamExecutionEnvironment setParallelism(int parallelism) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setParallelism method is unsupported.");
	}

	@Override
	public StreamExecutionEnvironment setMaxParallelism(int maxParallelism) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setMaxParallelism method is unsupported.");
	}

	@Override
	public int getParallelism() {
		return realExecEnv.getParallelism();
	}

	@Override
	public int getMaxParallelism() {
		return realExecEnv.getMaxParallelism();
	}

	@Override
	public StreamExecutionEnvironment setBufferTimeout(long timeoutMillis) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setBufferTimeout method is unsupported.");
	}

	@Override
	public long getBufferTimeout() {
		return realExecEnv.getBufferTimeout();
	}

	@Override
	public StreamExecutionEnvironment disableOperatorChaining() {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, disableOperatorChaining method is unsupported.");
	}

	@Override
	public boolean isChainingEnabled() {
		return realExecEnv.isChainingEnabled();
	}

	@Override
	public CheckpointConfig getCheckpointConfig() {
		return realExecEnv.getCheckpointConfig();
	}

	@Override
	public StreamExecutionEnvironment enableCheckpointing(long interval) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, enableCheckpointing method is unsupported.");
	}

	@Override
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, enableCheckpointing method is unsupported.");
	}

	@Override
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode, boolean force) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, enableCheckpointing method is unsupported.");
	}

	@Override
	public StreamExecutionEnvironment enableCheckpointing() {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, enableCheckpointing method is unsupported.");
	}

	@Override
	public long getCheckpointInterval() {
		return realExecEnv.getCheckpointInterval();
	}

	@Override
	public boolean isForceCheckpointing() {
		return realExecEnv.isForceCheckpointing();
	}

	@Override
	public CheckpointingMode getCheckpointingMode() {
		return realExecEnv.getCheckpointingMode();
	}

	@Override
	public StreamExecutionEnvironment setStateBackend(StateBackend backend) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setStateBackend method is unsupported.");
	}

	@Override
	public StateBackend getStateBackend() {
		return realExecEnv.getStateBackend();
	}

	@Override
	public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setRestartStrategy method is unsupported.");
	}

	@Override
	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		return realExecEnv.getRestartStrategy();
	}

	@Override
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setNumberOfExecutionRetries method is unsupported.");
	}

	@Override
	public int getNumberOfExecutionRetries() {
		return realExecEnv.getNumberOfExecutionRetries();
	}

	@Override
	public <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(Class<?> type, T serializer) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, addDefaultKryoSerializer method is unsupported.");
	}

	@Override
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, addDefaultKryoSerializer method is unsupported.");
	}

	@Override
	public <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, registerTypeWithKryoSerializer method is unsupported.");
	}

	@Override
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer> serializerClass) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, registerTypeWithKryoSerializer method is unsupported.");
	}

	@Override
	public void registerType(Class<?> type) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, registerType method is unsupported.");
	}

	@Override
	public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, setStreamTimeCharacteristic method is unsupported.");
	}

	@Override
	public TimeCharacteristic getStreamTimeCharacteristic() {
		return realExecEnv.getStreamTimeCharacteristic();
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, execute method is unsupported.");
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, execute method is unsupported.");
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, execute method is unsupported.");
	}

	@Override
	public void registerCachedFile(String filePath, String name) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, registerCachedFile method is unsupported.");
	}

	@Override
	public void registerCachedFile(String filePath, String name, boolean executable) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, registerCachedFile method is unsupported.");
	}

	@Override
	public StreamGraph getStreamGraph() {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, getStreamGraph method is unsupported.");
	}

	@Override
	public StreamGraph getStreamGraph(String jobName) {
		throw new UnsupportedOperationException(
			"This is a dummy StreamExecutionEnvironment, getStreamGraph method is unsupported.");
	}

}
