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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Serializer for {@link StreamRecord}. This version ignores timestamps and only deals with
 * the element.
 *
 * <p>
 * {@link MultiplexingStreamRecordSerializer} is a version that deals with timestamps and also
 * multiplexes {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks} in the same
 * stream with {@link StreamRecord StreamRecords}.
 *
 * @see MultiplexingStreamRecordSerializer
 *
 * @param <T> The type of value in the {@link StreamRecord}
 */
public class StreamRecordSerializer<T> extends TypeSerializer<Object> {

	private static final long serialVersionUID = 1L;

	protected final TypeSerializer<T> typeSerializer;

	public StreamRecordSerializer(TypeSerializer<T> serializer) {
		if (serializer instanceof StreamRecordSerializer) {
			throw new RuntimeException("StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: " + serializer);
		}
		this.typeSerializer = Preconditions.checkNotNull(serializer);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer duplicate() {
		return this;
	}

	@Override
	public Object createInstance() {
		try {
			return new StreamRecord<T>(typeSerializer.createInstance());
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate StreamRecord.", e);
		}
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Object copy(Object from) {
		StreamRecord<T> fromRecord = (StreamRecord<T>) from;
		return new StreamRecord<T>(typeSerializer.copy(fromRecord.getValue()), fromRecord.getTimestamp());
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object copy(Object from, Object reuse) {
		StreamRecord<T> fromRecord = (StreamRecord<T>) from;
		StreamRecord<T> reuseRecord = (StreamRecord<T>) reuse;

		reuseRecord.replace(typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue()), 0);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void serialize(Object value, DataOutputView target) throws IOException {
		StreamRecord<T> record = (StreamRecord<T>) value;
		typeSerializer.serialize(record.getValue(), target);
	}
	
	@Override
	public Object deserialize(DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(source);
		return new StreamRecord<T>(element, 0);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object deserialize(Object reuse, DataInputView source) throws IOException {
		StreamRecord<T> reuseRecord = (StreamRecord<T>) reuse;
		T element = typeSerializer.deserialize(reuseRecord.getValue(), source);
		reuseRecord.replace(element, 0);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		typeSerializer.copy(source, target);
	}
}
