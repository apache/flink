/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.util.SegmentsUtil;

import java.io.IOException;

/**
 * Serializer for {@link BinaryArray}.
 */
public class BinaryArraySerializer extends TypeSerializerSingleton<BinaryArray> {

	public static final BinaryArraySerializer INSTANCE = new BinaryArraySerializer();

	private BinaryArraySerializer() {}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public BinaryArray createInstance() {
		return new BinaryArray();
	}

	@Override
	public BinaryArray copy(BinaryArray from) {
		return from.copy();
	}

	@Override
	public BinaryArray copy(BinaryArray from, BinaryArray reuse) {
		return from.copy(reuse);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryArray record, DataOutputView target) throws IOException {
		target.writeInt(record.getSizeInBytes());
		SegmentsUtil.copyToView(record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
	}

	@Override
	public BinaryArray deserialize(DataInputView source) throws IOException {
		return deserialize(new BinaryArray(), source);
	}

	@Override
	public BinaryArray deserialize(BinaryArray reuse, DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public TypeSerializerSnapshot<BinaryArray> snapshotConfiguration() {
		return new BinaryArraySerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class BinaryArraySerializerSnapshot extends SimpleTypeSerializerSnapshot<BinaryArray> {

		public BinaryArraySerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
