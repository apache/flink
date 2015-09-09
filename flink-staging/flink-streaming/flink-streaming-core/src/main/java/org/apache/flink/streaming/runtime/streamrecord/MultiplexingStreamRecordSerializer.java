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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.IOException;

/**
 * Serializer for {@link StreamRecord} and {@link org.apache.flink.streaming.api.watermark.Watermark}. This does not behave like a normal
 * {@link TypeSerializer}, instead, this is only used at the
 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask} level for transmitting
 * {@link StreamRecord StreamRecords} and {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks}. This serializer
 * can handle both of them, therefore it returns {@link Object} the result has
 * to be cast to the correct type.
 *
 * @param <T> The type of value in the {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}
 */
public final class MultiplexingStreamRecordSerializer<T> extends TypeSerializer<StreamElement> {

	private static final long serialVersionUID = 1L;

	private static final long IS_WATERMARK = Long.MIN_VALUE;
	
	private final TypeSerializer<T> typeSerializer;

	
	public MultiplexingStreamRecordSerializer(TypeSerializer<T> serializer) {
		if (serializer instanceof MultiplexingStreamRecordSerializer || serializer instanceof StreamRecordSerializer) {
			throw new RuntimeException("StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: " + serializer);
		}
		this.typeSerializer = Preconditions.checkNotNull(serializer);
	}
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<StreamElement> duplicate() {
		TypeSerializer<T> copy = typeSerializer.duplicate();
		return (copy == typeSerializer) ? this : new MultiplexingStreamRecordSerializer<T>(copy);
	}

	@Override
	public StreamRecord<T> createInstance() {
		return new StreamRecord<T>(typeSerializer.createInstance(), 0L);
	}

	@Override
	public StreamElement copy(StreamElement from) {
		// we can reuse the timestamp since Instant is immutable
		if (from.isRecord()) {
			StreamRecord<T> fromRecord = from.asRecord();
			return new StreamRecord<T>(typeSerializer.copy(fromRecord.getValue()), fromRecord.getTimestamp());
		}
		else if (from.isWatermark()) {
			// is immutable
			return from;
		}
		else {
			throw new RuntimeException("Cannot copy " + from);
		}
	}

	@Override
	public StreamElement copy(StreamElement from, StreamElement reuse) {
		if (from.isRecord() && reuse.isRecord()) {
			StreamRecord<T> fromRecord = from.asRecord();
			StreamRecord<T> reuseRecord = reuse.asRecord();

			T valueCopy = typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
			reuseRecord.replace(valueCopy, fromRecord.getTimestamp());
			return reuse;
		}
		else if (from.isWatermark()) {
			// is immutable
			return from;
		}
		else {
			throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
		}
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StreamElement value, DataOutputView target) throws IOException {
		if (value.isRecord()) {
			StreamRecord<T> record = value.asRecord();
			target.writeLong(record.getTimestamp());
			typeSerializer.serialize(record.getValue(), target);
		}
		else if (value.isWatermark()) {
			target.writeLong(IS_WATERMARK);
			target.writeLong(value.asWatermark().getTimestamp());
		}
		else {
			throw new RuntimeException();
		}
	}
	
	@Override
	public StreamElement deserialize(DataInputView source) throws IOException {
		long millis = source.readLong();

		if (millis == IS_WATERMARK) {
			return new Watermark(source.readLong());
		}
		else {
			T element = typeSerializer.deserialize(source);
			return new StreamRecord<T>(element, millis);
		}
	}

	@Override
	public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
		long millis = source.readLong();

		if (millis == IS_WATERMARK) {
			return new Watermark(source.readLong());
		}
		else {
			StreamRecord<T> reuseRecord = reuse.asRecord();
			T element = typeSerializer.deserialize(reuseRecord.getValue(), source);
			reuseRecord.replace(element, millis);
			return reuse;
		}
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		long millis = source.readLong();
		target.writeLong(millis);

		if (millis == IS_WATERMARK) {
			target.writeLong(source.readLong());
		} else {
			typeSerializer.copy(source, target);
		}
	}
}
