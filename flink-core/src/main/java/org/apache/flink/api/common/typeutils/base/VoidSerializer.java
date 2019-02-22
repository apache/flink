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
 * Serializer for {@code Void}.
 */
@Internal
public final class VoidSerializer extends TypeSerializerSingleton<Void> {

	private static final long serialVersionUID = 1L;

	/** Sharable instance of the VoidSerializer. */
	public static final VoidSerializer INSTANCE = new VoidSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Void createInstance() {
		return null;
	}

	@Override
	public Void copy(Void from) {
		return null;
	}

	@Override
	public Void copy(Void from, Void reuse) {
		return null;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(Void record, DataOutputView target) throws IOException {
		// make progress in the stream, write one byte
		target.write(0);
	}

	@Override
	public Void deserialize(DataInputView source) throws IOException {
		source.readByte();
		return null;
	}

	@Override
	public Void deserialize(Void reuse, DataInputView source) throws IOException {
		source.readByte();
		return null;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source.readByte());
	}

	@Override
	public TypeSerializerSnapshot<Void> snapshotConfiguration() {
		return new VoidSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class VoidSerializerSnapshot extends SimpleTypeSerializerSnapshot<Void> {

		public VoidSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
