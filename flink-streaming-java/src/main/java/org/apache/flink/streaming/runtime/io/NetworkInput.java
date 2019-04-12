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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of taking {@link InputGate} as {@link Input}.
 */
@Internal
public final class NetworkInput implements Input, InputGateListener {

	private final int inputId;

	private final InputGate inputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/**
	 * Valves that control how watermarks and stream statuses from the input are forwarded.
	 */
	private final StatusWatermarkValve statusWatermarkValve;

	private final StreamStatusHandler streamStatusHandler;

	/** Registered listener to forward input notifications to. */
	private volatile CompletableFuture<?> inputListener = null;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to the correct channel index of the correct valve.
	 */
	private int currentChannel = -1;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	/** The new overall watermark aggregated on all aligned input channels. */
	private Watermark alignedWatermark = null;

	private final BitSet finishedChannels;

	private boolean isFinished = false;

	@SuppressWarnings("unchecked")
	public NetworkInput(
		int inputId,
		Collection<InputGate> inputGates,
		TypeSerializer<?> inputSerializer,
		StreamStatusHandler streamStatusHandler,
		IOManager ioManager) {

		this.inputId = inputId;

		this.inputGate = InputGateUtil.createInputGate(inputGates.toArray(new InputGate[0]));

		StreamElementSerializer<?> ser1 = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser1);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = new StatusWatermarkValve(
			inputGate.getNumberOfInputChannels(), new ForwardingValveOutputHandler());

		this.streamStatusHandler = checkNotNull(streamStatusHandler);

		this.finishedChannels = new BitSet(inputGate.getNumberOfInputChannels());

		// Register the network input as a listener for the input gate
		this.inputGate.registerListener(this);
	}

	@Override
	public int getId() {
		return inputId;
	}

	@Override
	public StreamElement pollNextElement() throws IOException, InterruptedException {

		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegate.getInstance();
					if (recordOrWatermark.isRecord()) {
						return recordOrWatermark;
					} else if (recordOrWatermark.isWatermark()) {
						statusWatermarkValve.inputWatermark(recordOrWatermark.asWatermark(), currentChannel);
						if (alignedWatermark == null) {
							continue;
						}

						StreamElement watermark = alignedWatermark;
						alignedWatermark = null;
						return watermark;
					} else if (recordOrWatermark.isStreamStatus()) {
						statusWatermarkValve.inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel);
						continue;
					} else {
						return recordOrWatermark;
					}
				}
			}

			//read the next buffer or event from the input gate
			Optional<BufferOrEvent> next = inputGate.pollNextBufferOrEvent();
			if (next.isPresent()) {
				final BufferOrEvent bufferOrEvent = next.get();

				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				} else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() == CheckpointBarrier.class || event.getClass() == CancelCheckpointMarker.class) {
						throw new UnsupportedOperationException("Checkpoint-related events are not supported currently.");
					} else {
						if (event.getClass() != EndOfPartitionEvent.class) {
							throw new IOException("Unexpected event: " + event);
						}

						int channelIndex = bufferOrEvent.getChannelIndex();
						finishedChannels.set(channelIndex);
						if (finishedChannels.cardinality() == inputGate.getNumberOfInputChannels()) {
							isFinished = true;
							return null;
						}
					}
				}
			} else {
				return null;
			}
		}
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public CompletableFuture<?> listen() {
		CompletableFuture<?> listener = inputListener;
		checkState(listener == null || listener.isDone());

		this.inputListener = new CompletableFuture<>();

		// fire immediately if the input has become available before then,
		// because in that case no "available" notification will be sent
		if (inputGate.moreAvailable()) {
			inputListener.complete(null);
		}

		return inputListener;
	}

	@Override
	public void unlisten() {
		CompletableFuture<?> listener = inputListener;
		if (listener != null && !listener.isDone()) {
			// wakes up all threads that are waiting on the input listener
			listener.cancel(true);
		}
	}

	@Override
	public void notifyInputGateNonEmpty(InputGate inputGate) {
		CompletableFuture<?> listener = inputListener;
		if (listener != null) {
			listener.complete(null);
		}
	}

	@Override
	public void close() {
		// if the listener is not completed, cancel it
		unlisten();

		// clear the buffers. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {

		@Override
		public void handleWatermark(Watermark watermark) {
			alignedWatermark = watermark;
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			streamStatusHandler.handleStreamStatus(NetworkInput.this.getId(), streamStatus);
		}
	}
}
