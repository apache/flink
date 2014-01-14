/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.api;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.channels.EndOfSuperstepEvent;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.serialization.RecordSerializer;
import eu.stratosphere.runtime.io.serialization.SpanningRecordSerializer;

import java.io.IOException;

/**
 * A record writer connects the application to an output gate. It allows the application
 * of emit (send out) to the output gate. The output gate will then take care of distributing
 * the emitted records among the output channels.
 * 
 * @param <T>
 *        the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> extends BufferWriter {

	private final BufferProvider bufferPool;

	private final ChannelSelector<T> channelSelector;

	private int numChannels;

	/** RecordSerializer per outgoing channel */
	private RecordSerializer<T>[] serializers;

	// -----------------------------------------------------------------------------------------------------------------

	public RecordWriter(AbstractTask task) {
		this((AbstractInvokable) task, new RoundRobinChannelSelector<T>());
	}

	public RecordWriter(AbstractTask task, ChannelSelector<T> channelSelector) {
		this((AbstractInvokable) task, channelSelector);
	}

	public RecordWriter(AbstractInputTask<?> task) {
		this((AbstractInvokable) task, new RoundRobinChannelSelector<T>());
	}

	public RecordWriter(AbstractInputTask<?> task, ChannelSelector<T> channelSelector) {
		this((AbstractInvokable) task, channelSelector);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private RecordWriter(AbstractInvokable invokable, ChannelSelector<T> channelSelector) {
		// initialize the gate
		super(invokable);

		this.bufferPool = invokable.getEnvironment().getOutputBufferProvider();
		this.channelSelector = channelSelector;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public void initializeSerializers() {
		this.numChannels = this.outputGate.getNumChannels();
		this.serializers = new RecordSerializer[numChannels];
		for (int i = 0; i < this.numChannels; i++) {
			this.serializers[i] = new SpanningRecordSerializer<T>();
		}
	}

	public void emit(final T record) throws IOException, InterruptedException {
		for (int targetChannel : this.channelSelector.selectChannels(record, this.numChannels)) {
			// serialize with corresponding serializer and send full buffer
			RecordSerializer<T> serializer = this.serializers[targetChannel];

			RecordSerializer.SerializationResult result = serializer.addRecord(record);
			while (result.isFullBuffer()) {
				Buffer buffer = serializer.getCurrentBuffer();
				if (buffer != null) {
					sendBuffer(buffer, targetChannel);
				}

				buffer = this.bufferPool.requestBufferBlocking(this.bufferPool.getBufferSize());
				result = serializer.setNextBuffer(buffer);
			}
		}
	}

	public void flush() throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < this.numChannels; targetChannel++) {
			RecordSerializer<T> serializer = this.serializers[targetChannel];

			Buffer buffer = serializer.getCurrentBuffer();
			if (buffer != null) {
				sendBuffer(buffer, targetChannel);
			}

			serializer.clear();
		}
	}

	@Override
	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < this.numChannels; targetChannel++) {
			RecordSerializer<T> serializer = this.serializers[targetChannel];

			Buffer buffer = serializer.getCurrentBuffer();
			if (buffer == null) {
				super.sendEvent(event, targetChannel);
			} else {
				super.sendBufferAndEvent(buffer, event, targetChannel);

				buffer = this.bufferPool.requestBufferBlocking(this.bufferPool.getBufferSize());
				serializer.setNextBuffer(buffer);
			}
		}
	}

	@Override
	public void sendEndOfSuperstep() throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < this.numChannels; targetChannel++) {
			RecordSerializer<T> serializer = this.serializers[targetChannel];

			Buffer buffer = serializer.getCurrentBuffer();
			if (buffer == null) {
				super.sendEvent(EndOfSuperstepEvent.INSTANCE, targetChannel);
			} else {
				super.sendBufferAndEvent(buffer, EndOfSuperstepEvent.INSTANCE, targetChannel);

				buffer = this.bufferPool.requestBufferBlocking(this.bufferPool.getBufferSize());
				serializer.setNextBuffer(buffer);
			}
		}
	}
}
