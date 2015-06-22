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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.joda.time.Instant;

public final class StreamRecordSerializer<T> extends TypeSerializer<StreamRecord<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> typeSerializer;

	public StreamRecordSerializer(TypeSerializer<T> serializer) {
		this.typeSerializer = serializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public StreamRecordSerializer<T> duplicate() {
		return this;
	}

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
		// we can reuse the timestamp since Instant is immutable
		return new StreamRecord<T>(typeSerializer.copy(from.getValue()), from.getTimestamp());
	}

	@Override
	public StreamRecord<T> copy(StreamRecord<T> from, StreamRecord<T> reuse) {
		reuse.replace(typeSerializer.copy(from.getValue(), reuse.getValue()), from.getTimestamp());
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StreamRecord<T> value, DataOutputView target) throws IOException {
		typeSerializer.serialize(value.getValue(), target);
		target.writeLong(value.getTimestamp().getMillis());
	}
	
	@Override
	public StreamRecord<T> deserialize(DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(source);
		long millis = source.readLong();
		return new StreamRecord<T>(element, new Instant(millis));
	}

	@Override
	public StreamRecord<T> deserialize(StreamRecord<T> reuse, DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(reuse.getValue(), source);
		long millis = source.readLong();
		reuse.replace(element, new Instant(millis));
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		typeSerializer.copy(source, target);
		target.writeLong(source.readLong());
	}
}
