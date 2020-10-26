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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.MockStreamStatusMaintainer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import javax.annotation.Nullable;

import java.util.function.BiConsumer;

/**
 * A builder of {@link MockStreamTask}.
 */
public class MockStreamTaskBuilder {
	private final Environment environment;
	private Object checkpointLock = new Object();
	private StreamConfig config;
	private ExecutionConfig executionConfig = new ExecutionConfig();
	private StreamStatusMaintainer streamStatusMaintainer = new MockStreamStatusMaintainer();
	private CheckpointStorageWorkerView checkpointStorage;
	private TimerService timerService = new TestProcessingTimeService();
	private StreamTaskStateInitializer streamTaskStateInitializer;
	private BiConsumer<String, Throwable> handleAsyncException = (message, throwable) -> { };
	private TaskMailbox taskMailbox = new TaskMailboxImpl();
	private StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor taskActionExecutor = StreamTaskActionExecutor.synchronizedExecutor();
	@Nullable
	private StreamInputProcessor inputProcessor;

	public MockStreamTaskBuilder(Environment environment) throws Exception {
		this.environment = environment;
		this.config = new StreamConfig(environment.getTaskConfiguration());

		StateBackend stateBackend = new MemoryStateBackend();
		this.checkpointStorage = stateBackend.createCheckpointStorage(new JobID());
		this.streamTaskStateInitializer = new StreamTaskStateInitializerImpl(environment, stateBackend);
	}

	public MockStreamTaskBuilder setCheckpointLock(Object checkpointLock) {
		this.checkpointLock = checkpointLock;
		return this;
	}

	public MockStreamTaskBuilder setConfig(StreamConfig config) {
		this.config = config;
		return this;
	}

	public MockStreamTaskBuilder setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
		return this;
	}

	public MockStreamTaskBuilder setStreamTaskStateInitializer(StreamTaskStateInitializer streamTaskStateInitializer) {
		this.streamTaskStateInitializer = streamTaskStateInitializer;
		return this;
	}

	public MockStreamTaskBuilder setStreamStatusMaintainer(StreamStatusMaintainer streamStatusMaintainer) {
		this.streamStatusMaintainer = streamStatusMaintainer;
		return this;
	}

	public MockStreamTaskBuilder setCheckpointStorage(CheckpointStorageAccess checkpointStorageAccess) {
		this.checkpointStorage = checkpointStorageAccess;
		return this;
	}

	public MockStreamTaskBuilder setTimerService(TimerService timerService) {
		this.timerService = timerService;
		return this;
	}

	public MockStreamTaskBuilder setHandleAsyncException(BiConsumer<String, Throwable> handleAsyncException) {
		this.handleAsyncException = handleAsyncException;
		return this;
	}

	public MockStreamTaskBuilder setTaskMailbox(TaskMailbox taskMailbox) {
		this.taskMailbox = taskMailbox;
		return this;
	}

	public MockStreamTaskBuilder setTaskActionExecutor(StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor taskActionExecutor) {
		this.taskActionExecutor = taskActionExecutor;
		return this;
	}

	public MockStreamTaskBuilder setStreamInputProcessor(StreamInputProcessor inputProcessor) {
		this.inputProcessor = inputProcessor;
		return this;
	}

	public MockStreamTask build() throws Exception {
		return new MockStreamTask(
			environment,
			checkpointLock,
			config,
			executionConfig,
			streamTaskStateInitializer,
			streamStatusMaintainer,
			checkpointStorage,
			timerService,
			handleAsyncException,
			taskMailbox,
			taskActionExecutor,
			inputProcessor);
	}
}
