/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A settable testing {@link StreamTask}.
 */
public class MockStreamTask extends StreamTask {

	private final String name;
	private final Object checkpointLock;
	private final StreamConfig config;
	private final ExecutionConfig executionConfig;
	private StreamTaskStateInitializer streamTaskStateInitializer;
	private final CloseableRegistry closableRegistry;
	private final StreamStatusMaintainer streamStatusMaintainer;
	private final CheckpointStorageWorkerView checkpointStorage;
	private final ProcessingTimeService processingTimeService;
	private final BiConsumer<String, Throwable> handleAsyncException;
	private final Map<String, Accumulator<?, ?>> accumulatorMap;

	public MockStreamTask(
		Environment environment,
		String name,
		Object checkpointLock,
		StreamConfig config,
		ExecutionConfig executionConfig,
		StreamTaskStateInitializer streamTaskStateInitializer,
		CloseableRegistry closableRegistry,
		StreamStatusMaintainer streamStatusMaintainer,
		CheckpointStorageWorkerView checkpointStorage,
		ProcessingTimeService processingTimeService,
		BiConsumer<String, Throwable> handleAsyncException,
		Map<String, Accumulator<?, ?>> accumulatorMap
	) {
		super(environment);
		this.name = name;
		this.checkpointLock = checkpointLock;
		this.config = config;
		this.executionConfig = executionConfig;
		this.streamTaskStateInitializer = streamTaskStateInitializer;
		this.closableRegistry = closableRegistry;
		this.streamStatusMaintainer = streamStatusMaintainer;
		this.checkpointStorage = checkpointStorage;
		this.processingTimeService = processingTimeService;
		this.handleAsyncException = handleAsyncException;
		this.accumulatorMap = accumulatorMap;
	}

	@Override
	public void init() { }

	@Override
	protected void processInput(ActionContext context) throws Exception {
		context.allActionsCompleted();
	}

	@Override
	protected void cleanup() { }

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Object getCheckpointLock() {
		return checkpointLock;
	}

	@Override
	public StreamConfig getConfiguration() {
		return config;
	}

	@Override
	public Environment getEnvironment() {
		return super.getEnvironment();
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return streamTaskStateInitializer;
	}

	public void setStreamTaskStateInitializer(StreamTaskStateInitializer streamTaskStateInitializer) {
		this.streamTaskStateInitializer = streamTaskStateInitializer;
	}

	@Override
	public CloseableRegistry getCancelables() {
		return closableRegistry;
	}

	@Override
	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return streamStatusMaintainer;
	}

	@Override
	public CheckpointStorageWorkerView getCheckpointStorage() {
		return checkpointStorage;
	}

	@Override
	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	@Override
	public void handleAsyncException(String message, Throwable exception) {
		handleAsyncException.accept(message, exception);
	}

	@Override
	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}
}
