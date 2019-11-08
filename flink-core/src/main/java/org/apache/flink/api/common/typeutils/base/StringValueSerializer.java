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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

@Internal
public final class StringValueSerializer extends TypeSerializerSingleton<StringValue> {

	private static final long serialVersionUID = 1L;
	
	private static final int HIGH_BIT = 0x1 << 7;
	
	public static final StringValueSerializer INSTANCE = new StringValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public StringValue createInstance() {
		return new StringValue();
	}

	@Override
	public StringValue copy(StringValue from) {
		return copy(from, new StringValue());
	}
	
	@Override
	public StringValue copy(StringValue from, StringValue reuse) {
		reuse.setValue(from);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StringValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public StringValue deserialize(DataInputView source) throws IOException {
		return deserialize(new StringValue(), source);
	}
	
	@Override
	public StringValue deserialize(StringValue reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int len = source.readUnsignedByte();
		target.writeByte(len);

		if (len >= HIGH_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7f;
			while ((curr = source.readUnsignedByte()) >= HIGH_BIT) {
				target.writeByte(curr);
				len |= (curr & 0x7f) << shift;
				shift += 7;
			}
			target.writeByte(curr);
			len |= curr << shift;
		}

		for (int i = 0; i < len; i++) {
			int c = source.readUnsignedByte();
			target.writeByte(c);
			while (c >= HIGH_BIT) {
				c = source.readUnsignedByte();
				target.writeByte(c);
			}
		}
	}

	@Override
	public TypeSerializerSnapshot<StringValue> snapshotConfiguration() {
		return new StringValueSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class StringValueSerializerSnapshot extends SimpleTypeSerializerSnapshot<StringValue> {

		public StringValueSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
