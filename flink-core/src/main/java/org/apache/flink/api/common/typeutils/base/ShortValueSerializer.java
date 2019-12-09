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
import org.apache.flink.types.ShortValue;

@Internal
public final class ShortValueSerializer extends TypeSerializerSingleton<ShortValue> {

	private static final long serialVersionUID = 1L;
	
	public static final ShortValueSerializer INSTANCE = new ShortValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ShortValue createInstance() {
		return new ShortValue();
	}

	@Override
	public ShortValue copy(ShortValue from) {
		return copy(from, new ShortValue());
	}
	
	@Override
	public ShortValue copy(ShortValue from, ShortValue reuse) {
		reuse.setValue(from.getValue());
		return reuse;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(ShortValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public ShortValue deserialize(DataInputView source) throws IOException {
		return deserialize(new ShortValue(), source);
	}
	
	@Override
	public ShortValue deserialize(ShortValue reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeShort(source.readShort());
	}

	@Override
	public TypeSerializerSnapshot<ShortValue> snapshotConfiguration() {
		return new ShortValueSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class ShortValueSerializerSnapshot extends SimpleTypeSerializerSnapshot<ShortValue> {

		public ShortValueSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
