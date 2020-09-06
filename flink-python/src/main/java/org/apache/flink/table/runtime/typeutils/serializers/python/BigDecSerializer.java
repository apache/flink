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
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * We create the BigDecSerializer instead of using the BigDecSerializer of flink-core module for
 * performance reasons in Python deserialization.
 */
@Internal
public class BigDecSerializer extends TypeSerializerSingleton<BigDecimal> {

	private static final long serialVersionUID = 1L;

	public static final BigDecSerializer INSTANCE = new BigDecSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public BigDecimal createInstance() {
		return BigDecimal.ZERO;
	}

	@Override
	public BigDecimal copy(BigDecimal from) {
		return from;
	}

	@Override
	public BigDecimal copy(BigDecimal from, BigDecimal reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BigDecimal record, DataOutputView target) throws IOException {
		byte[] bytes = StringUtf8Utils.encodeUTF8(record.toString());
		target.writeInt(bytes.length);
		target.write(bytes);
	}

	@Override
	public BigDecimal deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		byte[] bytes = new byte[size];
		source.readFully(bytes);
		return new BigDecimal(StringUtf8Utils.decodeUTF8(bytes, 0, size));
	}

	@Override
	public BigDecimal deserialize(BigDecimal reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializerSnapshot<BigDecimal> snapshotConfiguration() {
		return new BigDecSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class BigDecSerializerSnapshot extends SimpleTypeSerializerSnapshot<BigDecimal> {

		public BigDecSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
