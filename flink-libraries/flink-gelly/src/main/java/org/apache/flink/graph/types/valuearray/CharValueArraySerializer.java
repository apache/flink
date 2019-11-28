/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Specialized serializer for {@code CharValueArray}.
 */
public final class CharValueArraySerializer extends TypeSerializerSingleton<CharValueArray> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public CharValueArray createInstance() {
		return new CharValueArray();
	}

	@Override
	public CharValueArray copy(CharValueArray from) {
		return copy(from, new CharValueArray());
	}

	@Override
	public CharValueArray copy(CharValueArray from, CharValueArray reuse) {
		reuse.setValue(from);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(CharValueArray record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public CharValueArray deserialize(DataInputView source) throws IOException {
		return deserialize(new CharValueArray(), source);
	}

	@Override
	public CharValueArray deserialize(CharValueArray reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		CharValueArray.copyInternal(source, target);
	}

	// -----------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<CharValueArray> snapshotConfiguration() {
		return new CharValueArraySerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class CharValueArraySerializerSnapshot extends SimpleTypeSerializerSnapshot<CharValueArray> {

		public CharValueArraySerializerSnapshot() {
			super(CharValueArraySerializer::new);
		}
	}
}
