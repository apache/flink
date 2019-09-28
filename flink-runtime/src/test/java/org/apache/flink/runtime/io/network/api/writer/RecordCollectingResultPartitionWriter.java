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

import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.types.Record;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ResultPartitionWriter} that collects output on the List.
 */
public class RecordCollectingResultPartitionWriter extends AbstractCollectingResultPartitionWriter {
	private final List<Record> output;

	private final Record record = new Record();
	private final RecordDeserializer<Record> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(
		new String[]{System.getProperty("java.io.tmpdir")});

	public RecordCollectingResultPartitionWriter(List<Record> output, BufferProvider bufferProvider) {
		super(bufferProvider);
		this.output = checkNotNull(output);
	}

	@Override
	protected void deserializeBuffer(Buffer buffer) throws IOException {
		deserializer.setNextBuffer(buffer);

		while (deserializer.hasUnfinishedData()) {
			RecordDeserializer.DeserializationResult result = deserializer.getNextRecord(record);

			if (result.isFullRecord()) {
				output.add(record.createCopy());
			}

			if (result == RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER
				|| result == RecordDeserializer.DeserializationResult.PARTIAL_RECORD) {
				break;
			}
		}
	}
}
