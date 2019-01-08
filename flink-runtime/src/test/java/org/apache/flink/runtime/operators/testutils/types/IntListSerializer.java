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

package org.apache.flink.runtime.operators.testutils.types;

import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class IntListSerializer extends TypeSerializer<IntList> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public IntListSerializer duplicate() {
		return this;
	}
	
	@Override
	public IntList createInstance() {
		return new IntList();
	}
	
	@Override
	public IntList copy(IntList from) {
		return new IntList(from.getKey(), Arrays.copyOf(from.getValue(), from.getValue().length));
	}
	
	@Override
	public IntList copy(IntList from, IntList reuse) {
		reuse.setKey(from.getKey());
		reuse.setValue(Arrays.copyOf(from.getValue(), from.getValue().length));
		return reuse;
	}
	
	public IntList createCopy(IntList from) {
		return new IntList(from.getKey(), from.getValue());
	}

	public void copyTo(IntList from, IntList to) {
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(IntList record, DataOutputView target) throws IOException {
		target.writeInt(record.getKey());
		target.writeInt(record.getValue().length);
		for (int i = 0; i < record.getValue().length; i++) {
			target.writeInt(record.getValue()[i]);
		}
	}

	@Override
	public IntList deserialize(DataInputView source) throws IOException {
		return deserialize(new IntList(), source);
	}
	
	@Override
	public IntList deserialize(IntList record, DataInputView source) throws IOException {
		int key = source.readInt();
		record.setKey(key);
		int size = source.readInt();
		int[] value = new int[size];
		for (int i = 0; i < value.length; i++) {
			value[i] = source.readInt();
		}
		record.setValue(value);
		return record;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeInt(source.readInt());
		int len = source.readInt();
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			target.writeInt(source.readInt());
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntListSerializer) {
			IntListSerializer other = (IntListSerializer) obj;

			return other.canEqual(this);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof IntListSerializer;
	}

	@Override
	public int hashCode() {
		return IntListSerializer.class.hashCode();
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompatibilityResult<IntList> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		throw new UnsupportedOperationException();
	}
}
