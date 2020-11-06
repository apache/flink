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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

/**
 * The serializer of {@link ByteArrayWrapper}.
 */
@Internal
public class ByteArrayWrapperSerializer extends TypeSerializerSingleton<ByteArrayWrapper> {

	private static final long serialVersionUID = 1L;

	public static final ByteArrayWrapperSerializer INSTANCE = new ByteArrayWrapperSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ByteArrayWrapper createInstance() {
		return new ByteArrayWrapper(new byte[0]);
	}

	@Override
	public ByteArrayWrapper copy(ByteArrayWrapper from) {
		byte[] data = Arrays.copyOfRange(from.getData(), from.getOffset(), from.getLimit());
		return new ByteArrayWrapper(data);
	}

	@Override
	public ByteArrayWrapper copy(ByteArrayWrapper from, ByteArrayWrapper reuse) {
		byte[] data = Arrays.copyOfRange(from.getData(), from.getOffset(), from.getLimit());
		reuse.setData(data);
		reuse.setOffset(0);
		reuse.setLimit(data.length);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ByteArrayWrapper record, DataOutputView target) throws IOException {
		target.write(record.getLimit() - record.getOffset());
		target.write(record.getData(), record.getOffset(), record.getLimit() - record.getOffset());
	}

	@Override
	public ByteArrayWrapper deserialize(DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] result = new byte[length];
		source.read(result);
		return new ByteArrayWrapper(result);
	}

	@Override
	public ByteArrayWrapper deserialize(ByteArrayWrapper reuse, DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] result = new byte[length];
		source.read(result);
		reuse.setData(result);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		byte[] result = new byte[length];
		source.read(result);
		target.write(length);
		target.write(result);
	}

	@Override
	public TypeSerializerSnapshot<ByteArrayWrapper> snapshotConfiguration() {
		return new ByteArrayWrapperSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static class ByteArrayWrapperSerializerSnapshot extends SimpleTypeSerializerSnapshot<ByteArrayWrapper> {

		public ByteArrayWrapperSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
