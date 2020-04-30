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
 * Type serializer for {@code Integer} (and {@code int}, via auto-boxing).
 */
@Internal
public final class IntSerializer extends TypeSerializerSingleton<Integer> {

	private static final long serialVersionUID = 1L;

	/** Sharable instance of the IntSerializer. */
	public static final IntSerializer INSTANCE = new IntSerializer();

	private static final Integer ZERO = 0;

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Integer createInstance() {
		return ZERO;
	}

	@Override
	public Integer copy(Integer from) {
		return from;
	}

	@Override
	public Integer copy(Integer from, Integer reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(Integer record, DataOutputView target) throws IOException {
		target.writeInt(record);
	}

	@Override
	public Integer deserialize(DataInputView source) throws IOException {
		return source.readInt();
	}

	@Override
	public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeInt(source.readInt());
	}

	@Override
	public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
		return new IntSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	public static final class IntSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {

		@SuppressWarnings("WeakerAccess")
		public IntSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
