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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Type serializer for {@code Byte}.
 */
@Internal
public final class ShortSerializer extends TypeSerializerSingleton<Short> {

	private static final long serialVersionUID = 1L;

	/** Sharable instance of the ShortSerializer. */
	public static final ShortSerializer INSTANCE = new ShortSerializer();

	private static final Short ZERO = (short) 0;

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Short createInstance() {
		return ZERO;
	}

	@Override
	public Short copy(Short from) {
		return from;
	}

	@Override
	public Short copy(Short from, Short reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(Short record, DataOutputView target) throws IOException {
		target.writeShort(record);
	}

	@Override
	public Short deserialize(DataInputView source) throws IOException {
		return source.readShort();
	}

	@Override
	public Short deserialize(Short reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeShort(source.readShort());
	}

	@Override
	public TypeSerializerSnapshot<Short> snapshotConfiguration() {
		return new ShortSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class ShortSerializerSnapshot extends SimpleTypeSerializerSnapshot<Short> {

		public ShortSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
