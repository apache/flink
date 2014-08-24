/**
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

package org.apache.flink.streaming.api.streamrecord;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.TypeInformation;

public final class StreamRecordSerializer<T> extends TypeSerializer<StreamRecord<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> typeSerializer;
	private final boolean isTuple;

	public StreamRecordSerializer(TypeInformation<T> typeInfo) {
		this.typeSerializer = typeInfo.createSerializer();
		this.isTuple = typeInfo.isTupleType();
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public StreamRecord<T> createInstance() {
		try {
			@SuppressWarnings("unchecked")
			StreamRecord<T> t = StreamRecord.class.newInstance();
			t.isTuple = isTuple;
			t.setObject(typeSerializer.createInstance());
			return t;
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate StreamRecord.", e);
		}
	}

	@Override
	public StreamRecord<T> copy(StreamRecord<T> from, StreamRecord<T> reuse) {

		return null;
		// for (int i = 0; i < arity; i++) {
		// Object copy = fieldSerializers[i].copy(from.getField(i),
		// reuse.getField(i));
		// reuse.setField(copy, i);
		// }
		//
		// return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StreamRecord<T> value, DataOutputView target) throws IOException {
		value.getId().write(target);
		typeSerializer.serialize(value.getObject(), target);
	}

	@Override
	public StreamRecord<T> deserialize(StreamRecord<T> reuse, DataInputView source)
			throws IOException {
		reuse.getId().read(source);
		reuse.setObject(typeSerializer.deserialize(reuse.getObject(), source));
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

	}

	// @Override
	// public int hashCode() {
	// int hashCode = arity * 47;
	// for (TypeSerializer<?> ser : this.fieldSerializers) {
	// hashCode = (hashCode << 7) | (hashCode >>> -7);
	// hashCode += ser.hashCode();
	// }
	// return hashCode;
	// }

	// @Override
	// public boolean equals(Object obj) {
	// if (obj != null && obj instanceof StreamRecordSerializer) {
	// StreamRecordSerializer otherTS = (StreamRecordSerializer) obj;
	// return (otherTS.tupleClass == this.tupleClass)
	// && Arrays.deepEquals(this.fieldSerializers, otherTS.fieldSerializers);
	// } else {
	// return false;
	// }
	// }
}
