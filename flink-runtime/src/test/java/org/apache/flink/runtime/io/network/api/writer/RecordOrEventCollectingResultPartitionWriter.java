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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ResultPartitionWriter} that collects records or events on the List.
 */
public class RecordOrEventCollectingResultPartitionWriter<T> extends AbstractCollectingResultPartitionWriter {
	private final Collection<Object> output;
	private final NonReusingDeserializationDelegate<T> delegate;
	private final RecordDeserializer<DeserializationDelegate<T>> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<>
		(new String[]{System.getProperty("java.io.tmpdir")});

	public RecordOrEventCollectingResultPartitionWriter(
			Collection<Object> output,
			BufferProvider bufferProvider,
			TypeSerializer<T> serializer) {
		super(bufferProvider);
		this.output = checkNotNull(output);
		this.delegate = new NonReusingDeserializationDelegate<>(checkNotNull(serializer));
	}

	@Override
	protected void deserializeBuffer(Buffer buffer) throws IOException {
		if (buffer.isBuffer()) {
			deserializer.setNextBuffer(buffer);

			while (deserializer.hasUnfinishedData()) {
				RecordDeserializer.DeserializationResult result =
					deserializer.getNextRecord(delegate);

				if (result.isFullRecord()) {
					output.add(delegate.getInstance());
				}

				if (result == RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER
					|| result == RecordDeserializer.DeserializationResult.PARTIAL_RECORD) {
					break;
				}
			}
		} else {
			// is event
			AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
			output.add(event);
		}
	}
}
