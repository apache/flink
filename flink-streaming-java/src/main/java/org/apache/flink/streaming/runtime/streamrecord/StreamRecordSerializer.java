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
import org.apache.flink.annotation.Internal;
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
@Internal
public final class StreamRecordSerializer<T> extends TypeSerializer<StreamRecord<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> typeSerializer;
	

	public StreamRecordSerializer(TypeSerializer<T> serializer) {
		if (serializer instanceof StreamRecordSerializer) {
			throw new RuntimeException("StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: " + serializer);
		}
		this.typeSerializer = Preconditions.checkNotNull(serializer);
	}

	public TypeSerializer<T> getContainedTypeSerializer() {
		return this.typeSerializer;
	}
	
	// ------------------------------------------------------------------------
	//  General serializer and type utils
	// ------------------------------------------------------------------------

	@Override
	public StreamRecordSerializer<T> duplicate() {
		TypeSerializer<T> serializerCopy = typeSerializer.duplicate();
		return serializerCopy == typeSerializer ? this : new StreamRecordSerializer<T>(serializerCopy);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return typeSerializer.getLength();
	}

	// ------------------------------------------------------------------------
	//  Type serialization, copying, instantiation
	// ------------------------------------------------------------------------

	@Override
	public StreamRecord<T> createInstance() {
		try {
			return new StreamRecord<T>(typeSerializer.createInstance());
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate StreamRecord.", e);
		}
	}
	
	@Override
	public StreamRecord<T> copy(StreamRecord<T> from) {
		return new StreamRecord<T>(typeSerializer.copy(from.getValue()), from.getTimestamp());
	}

	@Override
	public StreamRecord<T> copy(StreamRecord<T> from, StreamRecord<T> reuse) {
		reuse.replace(typeSerializer.copy(from.getValue(), reuse.getValue()), 0);
		return reuse;
	}

	@Override
	public void serialize(StreamRecord<T> value, DataOutputView target) throws IOException {
		typeSerializer.serialize(value.getValue(), target);
	}
	
	@Override
	public StreamRecord<T> deserialize(DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(source);
		return new StreamRecord<T>(element, 0);
	}

	@Override
	public StreamRecord<T> deserialize(StreamRecord<T> reuse, DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(reuse.getValue(), source);
		reuse.replace(element, 0);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		typeSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamRecordSerializer) {
			StreamRecordSerializer<?> other = (StreamRecordSerializer<?>) obj;

			return other.canEqual(this) && typeSerializer.equals(other.typeSerializer);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StreamRecordSerializer;
	}

	@Override
	public int hashCode() {
		return typeSerializer.hashCode();
	}
}
