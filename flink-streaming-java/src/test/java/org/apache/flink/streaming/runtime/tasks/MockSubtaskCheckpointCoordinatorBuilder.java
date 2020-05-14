/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorage;
import org.apache.flink.util.function.BiFunctionWithException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor.IMMEDIATE;

/**
 * A mock builder to build {@link SubtaskCheckpointCoordinator}.
 */
public class MockSubtaskCheckpointCoordinatorBuilder {
	private String taskName = "mock-task";
	private CheckpointStorageWorkerView checkpointStorage;
	private Environment environment;
	private AsyncExceptionHandler asyncExceptionHandler;
	private StreamTaskActionExecutor actionExecutor = IMMEDIATE;
	private CloseableRegistry closeableRegistry = new CloseableRegistry();
	private ExecutorService executorService = Executors.newDirectExecutorService();
	private BiFunctionWithException<ChannelStateWriter, Long, CompletableFuture<Void>, IOException> prepareInputSnapshot = (channelStateWriter, aLong) -> FutureUtils.completedVoidFuture();
	private boolean unalignedCheckpointEnabled;

	public MockSubtaskCheckpointCoordinatorBuilder setEnvironment(Environment environment) {
		this.environment = environment;
		return this;
	}

	public MockSubtaskCheckpointCoordinatorBuilder setPrepareInputSnapshot(BiFunctionWithException<ChannelStateWriter, Long, CompletableFuture<Void>, IOException> prepareInputSnapshot) {
		this.prepareInputSnapshot = prepareInputSnapshot;
		return this;
	}

	public MockSubtaskCheckpointCoordinatorBuilder setUnalignedCheckpointEnabled(boolean unalignedCheckpointEnabled) {
		this.unalignedCheckpointEnabled = unalignedCheckpointEnabled;
		return this;
	}

	SubtaskCheckpointCoordinator build() throws IOException {
		if (environment == null) {
			this.environment = MockEnvironment.builder().build();
		}
		if (checkpointStorage == null) {
			this.checkpointStorage = new MemoryBackendCheckpointStorage(environment.getJobID(), null, null, 1024);
		}
		if (asyncExceptionHandler == null) {
			this.asyncExceptionHandler = new NonHandleAsyncException();
		}

		return new SubtaskCheckpointCoordinatorImpl(
			checkpointStorage,
			taskName,
			actionExecutor,
			closeableRegistry,
			executorService,
			environment,
			asyncExceptionHandler,
			unalignedCheckpointEnabled,
			prepareInputSnapshot);
	}

	private static class NonHandleAsyncException implements AsyncExceptionHandler {

		@Override
		public void handleAsyncException(String message, Throwable exception) {
			// do nothing.
		}
	}
}
