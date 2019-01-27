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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BinaryString;

import java.io.IOException;

/**
 * Serializer for {@link BinaryString}.
 */
@Internal
public final class BinaryStringSerializer extends TypeSerializerSingleton<BinaryString> {

	private static final long serialVersionUID = 1L;

	public static final BinaryStringSerializer INSTANCE = new BinaryStringSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public BinaryString createInstance() {
		return BinaryString.EMPTY_UTF8;
	}

	@Override
	public BinaryString copy(BinaryString from) {
		return from.copy();
	}

	@Override
	public BinaryString copy(BinaryString from, BinaryString reuse) {
		return from.copy(reuse);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryString record, DataOutputView target) throws IOException {
		byte[] bytes = record.getBytes();
		target.writeInt(bytes.length);
		target.write(bytes);
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
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		target.write(bytes);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BinaryStringSerializer;
	}
}
