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
import org.apache.flink.types.CharValue;

@Internal
public class CharValueSerializer extends TypeSerializerSingleton<CharValue> {

	private static final long serialVersionUID = 1L;
	
	public static final CharValueSerializer INSTANCE = new CharValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public CharValue createInstance() {
		return new CharValue();
	}
	
	@Override
	public CharValue copy(CharValue from) {
		return copy(from, new CharValue());
	}

	@Override
	public CharValue copy(CharValue from, CharValue reuse) {
		reuse.setValue(from.getValue());
		return reuse;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(CharValue record, DataOutputView target) throws IOException {
		record.write(target);
	}
	
	@Override
	public CharValue deserialize(DataInputView source) throws IOException {
		return deserialize(new CharValue(), source);
	}

	@Override
	public CharValue deserialize(CharValue reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeChar(source.readChar());
	}

	@Override
	public TypeSerializerSnapshot<CharValue> snapshotConfiguration() {
		return new CharValueSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class CharValueSerializerSnapshot extends SimpleTypeSerializerSnapshot<CharValue> {

		public CharValueSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
