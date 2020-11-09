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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link StreamInputProcessor} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	private final CheckpointedInputGate checkpointedInputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private final Map<InputChannelInfo, Integer> channelIndexes;

	private int lastChannel = UNSPECIFIED;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	@SuppressWarnings("unchecked")
	public StreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[checkpointedInputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;
		this.channelIndexes = getChannelIndexes(checkpointedInputGate);
	}

	@Nonnull
	private static Map<InputChannelInfo, Integer> getChannelIndexes(CheckpointedInputGate checkpointedInputGate) {
		int index = 0;
		List<InputChannelInfo> channelInfos = checkpointedInputGate.getChannelInfos();
		Map<InputChannelInfo, Integer> channelIndexes = new HashMap<>(channelInfos.size());
		for (InputChannelInfo channelInfo : channelInfos) {
			channelIndexes.put(channelInfo, index++);
		}
		return channelIndexes;
	}

	@VisibleForTesting
	StreamTaskNetworkInput(
		CheckpointedInputGate checkpointedInputGate,
		TypeSerializer<?> inputSerializer,
		StatusWatermarkValve statusWatermarkValve,
		int inputIndex,
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers) {

		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));
		this.recordDeserializers = recordDeserializers;
		this.statusWatermarkValve = statusWatermarkValve;
		this.inputIndex = inputIndex;
		this.channelIndexes = getChannelIndexes(checkpointedInputGate);
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result;
				try {
					result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				} catch (IOException e) {
					throw new IOException(String.format("Can't get next record for channel %d", lastChannel), e);
				}
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}

			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			if (bufferOrEvent.isPresent()) {
				// return to the mailbox after receiving a checkpoint barrier to avoid processing of
				// data after the barrier before checkpoint is performed for unaligned checkpoint mode
				if (bufferOrEvent.get().isBuffer()) {
					processBuffer(bufferOrEvent.get());
				} else {
					processEvent(bufferOrEvent.get());
					return InputStatus.MORE_AVAILABLE;
				}
			} else {
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel, output);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel, output);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processEvent(BufferOrEvent bufferOrEvent) {
		// Event received
		final AbstractEvent event = bufferOrEvent.getEvent();
		// TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
		if (event.getClass() == EndOfPartitionEvent.class) {
			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(channelIndexes.get(bufferOrEvent.getChannelInfo()));
		}
	}

	private void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
		lastChannel = channelIndexes.get(bufferOrEvent.getChannelInfo());
		checkState(lastChannel != StreamTaskInput.UNSPECIFIED);
		currentRecordDeserializer = recordDeserializers[lastChannel];
		checkState(currentRecordDeserializer != null,
			"currentRecordDeserializer has already been released");

		currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentRecordDeserializer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) throws IOException {
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
			if (deserializer != null) {
				final InputChannel channel = checkpointedInputGate.getChannel(channelIndex);

				channelStateWriter.addInputData(
					checkpointId,
					channel.getChannelInfo(),
					ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
					deserializer.getUnconsumedBuffer());
			}
		}
		return checkpointedInputGate.getAllBarriersReceivedFuture(checkpointId);
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.close();
	}

	private void releaseDeserializer(int channelIndex) {
		RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
		if (deserializer != null) {
			// recycle buffers and clear the deserializer.
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();

			recordDeserializers[channelIndex] = null;
		}
	}
}
