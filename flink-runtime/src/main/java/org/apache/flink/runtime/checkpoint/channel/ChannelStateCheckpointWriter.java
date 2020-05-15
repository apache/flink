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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes channel state for a specific checkpoint-subtask-attempt triple.
 */
@NotThreadSafe
class ChannelStateCheckpointWriter {
	private static final Logger LOG = LoggerFactory.getLogger(ChannelStateCheckpointWriter.class);

	private final DataOutputStream dataStream;
	private final CheckpointStateOutputStream checkpointStream;
	private final ChannelStateWriteResult result;
	private final Map<InputChannelInfo, List<Long>> inputChannelOffsets = new HashMap<>();
	private final Map<ResultSubpartitionInfo, List<Long>> resultSubpartitionOffsets = new HashMap<>();
	private final ChannelStateSerializer serializer;
	private final long checkpointId;
	private boolean allInputsReceived = false;
	private boolean allOutputsReceived = false;
	private final RunnableWithException onComplete;

	ChannelStateCheckpointWriter(
			CheckpointStartRequest startCheckpointItem,
			CheckpointStreamFactory streamFactory,
			ChannelStateSerializer serializer,
			RunnableWithException onComplete) throws Exception {
		this(
			startCheckpointItem.getCheckpointId(),
			startCheckpointItem.getTargetResult(),
			streamFactory.createCheckpointStateOutputStream(EXCLUSIVE),
			serializer,
			onComplete);
	}

	ChannelStateCheckpointWriter(
			long checkpointId,
			ChannelStateWriteResult result,
			CheckpointStateOutputStream stream,
			ChannelStateSerializer serializer,
			RunnableWithException onComplete) throws Exception {
		this(checkpointId, result, serializer, onComplete, stream, new DataOutputStream(stream));
	}

	ChannelStateCheckpointWriter(
			long checkpointId,
			ChannelStateWriteResult result,
			ChannelStateSerializer serializer,
			RunnableWithException onComplete,
			CheckpointStateOutputStream checkpointStateOutputStream,
			DataOutputStream dataStream) throws Exception {
		this.checkpointId = checkpointId;
		this.result = checkNotNull(result);
		this.checkpointStream = checkNotNull(checkpointStateOutputStream);
		this.serializer = checkNotNull(serializer);
		this.dataStream = checkNotNull(dataStream);
		this.onComplete = checkNotNull(onComplete);
		runWithChecks(() -> serializer.writeHeader(dataStream));
	}

	void writeInput(InputChannelInfo info, Buffer... flinkBuffers) throws Exception {
		write(inputChannelOffsets, info, flinkBuffers, !allInputsReceived);
	}

	void writeOutput(ResultSubpartitionInfo info, Buffer... flinkBuffers) throws Exception {
		write(resultSubpartitionOffsets, info, flinkBuffers, !allOutputsReceived);
	}

	private <K> void write(Map<K, List<Long>> offsets, K key, Buffer[] flinkBuffers, boolean precondition) throws Exception {
		try {
			if (result.isDone()) {
				return;
			}
			runWithChecks(() -> {
				checkState(precondition);
				offsets
					.computeIfAbsent(key, unused -> new ArrayList<>())
					.add(checkpointStream.getPos());
				serializer.writeData(dataStream, flinkBuffers);
			});
		} finally {
			for (Buffer flinkBuffer : flinkBuffers) {
				flinkBuffer.recycleBuffer();
			}
		}
	}

	void completeInput() throws Exception {
		LOG.debug("complete input, output completed: {}", allOutputsReceived);
		complete(!allInputsReceived, () -> allInputsReceived = true);
	}

	void completeOutput() throws Exception {
		LOG.debug("complete output, input completed: {}", allInputsReceived);
		complete(!allOutputsReceived, () -> allOutputsReceived = true);
	}

	private void complete(boolean precondition, RunnableWithException complete) throws Exception {
		if (result.isDone()) {
			// likely after abort - only need to set the flag run onComplete callback
			doComplete(precondition, complete, onComplete);
		} else {
			runWithChecks(() -> doComplete(precondition, complete, onComplete, this::finishWriteAndResult));
		}
	}

	private void finishWriteAndResult() throws IOException {
		if (inputChannelOffsets.isEmpty() && resultSubpartitionOffsets.isEmpty()) {
			dataStream.close();
			result.inputChannelStateHandles.complete(emptyList());
			result.resultSubpartitionStateHandles.complete(emptyList());
			return;
		}
		dataStream.flush();
		StreamStateHandle underlying = checkpointStream.closeAndGetHandle();
		complete(
				result.inputChannelStateHandles,
				inputChannelOffsets,
				(chan, offsets) -> new InputChannelStateHandle(chan, underlying, offsets));
		complete(
				result.resultSubpartitionStateHandles,
				resultSubpartitionOffsets,
				(chan, offsets) -> new ResultSubpartitionStateHandle(chan, underlying, offsets));
	}

	private void doComplete(boolean precondition, RunnableWithException complete, RunnableWithException... callbacks) throws Exception {
		Preconditions.checkArgument(precondition);
		complete.run();
		if (allInputsReceived && allOutputsReceived) {
			for (RunnableWithException callback : callbacks) {
				callback.run();
			}
		}
	}

	private <I, H extends AbstractChannelStateHandle<I>> void complete(
			CompletableFuture<Collection<H>> future,
			Map<I, List<Long>> offsets,
			BiFunction<I, List<Long>, H> buildHandle) {
		final Collection<H> handles = new ArrayList<>();
		for (Map.Entry<I, List<Long>> e : offsets.entrySet()) {
			handles.add(buildHandle.apply(e.getKey(), e.getValue()));
		}
		future.complete(handles);
		LOG.debug("channel state write completed, checkpointId: {}, handles: {}", checkpointId, handles);
	}

	private void runWithChecks(RunnableWithException r) throws Exception {
		try {
			checkState(!result.isDone(), "result is already completed", result);
			r.run();
		} catch (Exception e) {
			fail(e);
			throw e;
		}
	}

	public void fail(Throwable e) throws Exception {
		result.fail(e);
		checkpointStream.close();
	}

}
