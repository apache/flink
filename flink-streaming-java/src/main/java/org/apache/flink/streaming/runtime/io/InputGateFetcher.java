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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManagerUtility;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.InputSelector.InputSelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Input gate fetcher.
 *
 * @param <IN> the type parameter
 */
class InputGateFetcher<IN> implements InputFetcher, InputGateListener {

	private static final Logger LOG = LoggerFactory.getLogger(InputGateFetcher.class);

	private final InputGate inputGate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final SelectedReadingBarrierHandler barrierHandler;

	private final InputProcessor inputProcessor;

	private final Object checkpointLock;

	private final int basedChannelCount;

	private final InputSelection inputSelection;

	private IN reusedObject;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private int currentChannelIndex;

	private boolean isFinished = false;

	private InputFetcherAvailableListener listener;

	/**
	 * Instantiates a new Input gate fetcher.
	 *
	 * @param inputSelection 	the input selection of this input fetcher
	 * @param inputGate         the input gate
	 * @param serializer        the serializer
	 * @param barrierHandler    the barrier handler
	 * @param ioManager         the io manager
	 * @param inputProcessor    the input processor
	 * @param checkpointLock    the checkpoint lock
	 * @param basedChannelCount the based channel count
	 * @param objectReuse 		the object is reusable or not
	 */
	public InputGateFetcher(
		InputSelection inputSelection,
		InputGate inputGate,
		TypeSerializer<IN> serializer,
		SelectedReadingBarrierHandler barrierHandler,
		IOManager ioManager,
		InputProcessor inputProcessor,
		Object checkpointLock,
		int basedChannelCount,
		boolean objectReuse,
		Configuration taskManagerConfig) {

		this.inputSelection = checkNotNull(inputSelection);
		this.inputGate = checkNotNull(inputGate);
		this.barrierHandler = checkNotNull(barrierHandler);
		this.inputProcessor = checkNotNull(inputProcessor);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.basedChannelCount = basedChannelCount;

		// Initialize one deserializer per input channel
		//noinspection unchecked
		SerializerManagerUtility<DeserializationDelegate<StreamElement>> serializerManagerUtility =
			new SerializerManagerUtility<>(taskManagerConfig);
		this.recordDeserializers = serializerManagerUtility.createRecordDeserializers(
			inputGate.getAllInputChannels(), ioManager.getSpillingDirectoriesPaths());

		if (objectReuse) {
			reusedObject = serializer.createInstance();
			this.deserializationDelegate = new ReusingDeserializationDelegate<>(new StreamElementSerializer<>(serializer));
		} else {
			reusedObject = null;
			this.deserializationDelegate = new NonReusingDeserializationDelegate<>(new StreamElementSerializer<>(serializer));
		}

		this.inputGate.registerListener(this);
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public boolean moreAvailable() {
		return currentRecordDeserializer != null || inputGate.moreAvailable();
	}

	@Override
	public void setup() throws Exception {
		inputGate.requestPartitions();
	}

	@Override
	public boolean fetchAndProcess() throws Exception {

		while (true) {
			DeserializationResult result = getNextResult();
			if (result == null) {
				return false;
			} else if (result.isFullRecord()) {
				final StreamElement streamElement = deserializationDelegate.getInstance();
				if (streamElement.isRecord()) {
					final StreamRecord<IN> record = streamElement.asRecord();
					reusedObject = record.getValue();
					inputProcessor.processRecord(record, currentChannelIndex);
				} else if (streamElement.isWatermark()) {
					inputProcessor.processWatermark(streamElement.asWatermark(), currentChannelIndex);
				} else if (streamElement.isLatencyMarker()) {
					inputProcessor.processLatencyMarker(streamElement.asLatencyMarker(), currentChannelIndex);
				} else if (streamElement.isStreamStatus()) {
					inputProcessor.processStreamStatus(streamElement.asStreamStatus(), currentChannelIndex);
				} else {
					throw new RuntimeException("Unknown stream element " + streamElement);
				}

				return true;
			}
		}
	}

	/**
	 * Gets next result.
	 *
	 * @return the next result
	 * @throws Exception the exception
	 */
	DeserializationResult getNextResult() throws Exception {
		if (currentRecordDeserializer != null) {
			// If there is some record in memory
			if (reusedObject != null) {
				this.deserializationDelegate.setInstance(new StreamRecord<>(reusedObject));
			}
			final DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
			if (result.isBufferConsumed()) {
				currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
				currentRecordDeserializer = null;
			}
			return result;
		} else {
			// Need to fetch from input gate
			final BufferOrEvent bufferOrEvent = getNextBufferOrEvent();
			if (bufferOrEvent != null) {
				if (reusedObject != null) {
					this.deserializationDelegate.setInstance(new StreamRecord<>(reusedObject));
				}
				final DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}
				return result;
			} else {
				return null;
			}
		}
	}

	/**
	 * Gets next buffer or event.
	 *
	 * @return the next buffer or null if there is no data available currently
	 * @throws Exception the exception
	 */
	@VisibleForTesting
	BufferOrEvent getNextBufferOrEvent() throws Exception {
		BufferOrEvent bufferOrEvent;
		while ((bufferOrEvent = barrierHandler.pollNext(inputGate)) != null) {
			if (bufferOrEvent.isEvent()) {
				checkArgument(bufferOrEvent.getEvent() instanceof EndOfPartitionEvent);

				LOG.info("receive event:" + bufferOrEvent.getEvent());
				if (inputGate.isFinished()) {
					LOG.info("Input gate {} is finished", inputGate);
					synchronized (checkpointLock) {
						inputProcessor.endInput();
						inputProcessor.release();
					}
					isFinished = true;
					return null;
				}
			} else {
				currentChannelIndex = bufferOrEvent.getChannelIndex() - basedChannelCount;
				currentRecordDeserializer = this.recordDeserializers[currentChannelIndex];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				return bufferOrEvent;
			}
		}
		return null;
	}

	@VisibleForTesting
	RecordDeserializer<DeserializationDelegate<StreamElement>> getCurrentRecordDeserializer() {
		return currentRecordDeserializer;
	}

	@VisibleForTesting
	void setCurrentRecordDeserializer(RecordDeserializer<DeserializationDelegate<StreamElement>> recordDeserializer) {
		this.currentRecordDeserializer = recordDeserializer;
	}

	@VisibleForTesting
	int getCurrentChannelIndex() {
		return currentChannelIndex;
	}

	@VisibleForTesting
	DeserializationDelegate<StreamElement> getDeserializationDelegate() {
		return deserializationDelegate;
	}

	@VisibleForTesting
	IN getReusedObject() {
		return reusedObject;
	}

	public InputGate getInputGate() {
		return inputGate;
	}

	@Override
	public void cleanup() {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}

	@Override
	public void cancel() {

	}

	@Override
	public InputSelection getInputSelection() {
		return inputSelection;
	}

	@Override
	public void registerAvailableListener(InputFetcherAvailableListener listener) {
		checkState(this.listener == null);
		this.listener = listener;
	}

	@Override
	public void notifyInputGateNonEmpty(InputGate inputGate) {
		checkArgument(this.inputGate == inputGate);
		if (listener != null) {
			listener.notifyInputFetcherAvailable(this);
		}
	}
}
