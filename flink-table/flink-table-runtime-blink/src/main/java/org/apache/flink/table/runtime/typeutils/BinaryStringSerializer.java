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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.runtime.util.SegmentsUtil;

import java.io.IOException;

/**
 * Serializer for {@link BinaryString}.
 */
@Internal
public final class BinaryStringSerializer extends TypeSerializerSingleton<BinaryString> {

	private static final long serialVersionUID = 1L;

	public static final BinaryStringSerializer INSTANCE = new BinaryStringSerializer();

	private BinaryStringSerializer() {}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public BinaryString createInstance() {
		return BinaryString.fromString("");
	}

	@Override
	public BinaryString copy(BinaryString from) {
		return from.copy();
	}

	@Override
	public BinaryString copy(BinaryString from, BinaryString reuse) {
		return from.copy();
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryString record, DataOutputView target) throws IOException {
		record.ensureMaterialized();
		target.writeInt(record.getSizeInBytes());
		SegmentsUtil.copyToView(record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
	}

	@Override
	public BinaryString deserialize(DataInputView source) throws IOException {
		return deserializeInternal(source);
	}

	public static BinaryString deserializeInternal(DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		return BinaryString.fromBytes(bytes);
	}

	@Override
	public BinaryString deserialize(BinaryString record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public TypeSerializerSnapshot<BinaryString> snapshotConfiguration() {
		return new BinaryStringSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class BinaryStringSerializerSnapshot extends SimpleTypeSerializerSnapshot<BinaryString> {

		public BinaryStringSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
