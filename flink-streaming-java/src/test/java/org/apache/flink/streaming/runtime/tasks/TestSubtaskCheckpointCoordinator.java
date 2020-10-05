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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.TestCheckpointStorageWorkerView;

import java.util.function.Supplier;

/**
 * {@link SubtaskCheckpointCoordinator} implementation for tests.
 */
public class TestSubtaskCheckpointCoordinator implements SubtaskCheckpointCoordinator {

	public static final TestSubtaskCheckpointCoordinator INSTANCE = new TestSubtaskCheckpointCoordinator();

	private static final int DEFAULT_MAX_STATE_SIZE = 1000;

	private final CheckpointStorageWorkerView storageWorkerView;
	private final ChannelStateWriter channelStateWriter;

	private TestSubtaskCheckpointCoordinator() {
		this(new TestCheckpointStorageWorkerView(DEFAULT_MAX_STATE_SIZE), ChannelStateWriter.NO_OP);
	}

	public TestSubtaskCheckpointCoordinator(ChannelStateWriter channelStateWriter) {
		this(new TestCheckpointStorageWorkerView(DEFAULT_MAX_STATE_SIZE), channelStateWriter);
	}

	private TestSubtaskCheckpointCoordinator(CheckpointStorageWorkerView storageWorkerView, ChannelStateWriter channelStateWriter) {
		this.storageWorkerView = storageWorkerView;
		this.channelStateWriter = channelStateWriter;
	}

	@Override
	public void initCheckpoint(long id, CheckpointOptions checkpointOptions) {
		channelStateWriter.start(id, checkpointOptions);
	}

	@Override
	public ChannelStateWriter getChannelStateWriter() {
		return channelStateWriter;
	}

	@Override
	public CheckpointStorageWorkerView getCheckpointStorage() {
		return storageWorkerView;
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause, OperatorChain<?, ?> operatorChain) {
		channelStateWriter.abort(checkpointId, cause, true);
	}

	@Override
	public void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetricsBuilder checkpointMetrics,
			OperatorChain<?, ?> operatorChain,
			Supplier<Boolean> isCanceled) {
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning) {
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning) {
	}

	@Override
	public void close() {
	}
}
