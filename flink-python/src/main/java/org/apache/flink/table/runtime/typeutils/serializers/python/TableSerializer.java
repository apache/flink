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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;

/**
 * Base Table Serializer for Table Function.
 */
@Internal
abstract class TableSerializer<T> extends TypeSerializer<T> {

	private final TypeSerializer[] fieldSerializers;

	private transient boolean[] nullMask;

	TableSerializer(TypeSerializer[] fieldSerializers) {
		this.fieldSerializers = fieldSerializers;
		this.nullMask = new boolean[Math.max(fieldSerializers.length - 8, 0)];
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<T> duplicate() {
		throw new RuntimeException("This method duplicate() should not be called");
	}

	@Override
	public T createInstance() {
		return unwantedMethodCall("createInstance()");
	}

	@Override
	public T copy(T from) {
		return unwantedMethodCall("copy(T from)");
	}

	@Override
	public T copy(T from, T reuse) {
		return unwantedMethodCall("copy(T from, T reuse)");
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return unwantedMethodCall("deserialize(T reuse, DataInputView source)");
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		unwantedMethodCall("copy(DataInputView source, DataOutputView target)");
	}

	private T unwantedMethodCall(String methodName) {
		throw new RuntimeException(String.format("The method %s should not be called", methodName));
	}

	public abstract T createResult(int len);

	public abstract void setField(T result, int index, Object value);

	@Override
	public T deserialize(DataInputView source) throws IOException {
		int len = fieldSerializers.length;
		int b = source.readUnsignedByte() & 0xff;
		DataInputStream inputStream = (DataInputStream) source;
		if (b == 0x00 && inputStream.available() == 0) {
			return createResult(0);
		}
		T result = createResult(len);
		int minLen = Math.min(8, len);
		readIntoNullMask(len - 8, source, nullMask);
		for (int i = 0; i < minLen; i++) {
			if ((b & 0x80) > 0) {
				setField(result, i, null);
			} else {
				setField(result, i, fieldSerializers[i].deserialize(source));
			}
			b = b << 1;
		}
		for (int i = 0, j = minLen; j < len; i++, j++) {
			if (nullMask[i]) {
				setField(result, j, null);
			} else {
				setField(result, j, fieldSerializers[j].deserialize(source));
			}
		}

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TableSerializer) {
			TableSerializer other = (TableSerializer) obj;
			if (this.fieldSerializers.length == other.fieldSerializers.length) {
				for (int i = 0; i < this.fieldSerializers.length; i++) {
					if (!this.fieldSerializers[i].equals(other.fieldSerializers[i])) {
						return false;
					}
				}
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(fieldSerializers);
	}

	@Override
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		throw new RuntimeException("The method snapshotConfiguration() should not be called");
	}
}
