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

package org.apache.flink.api.common.typeutils.base.array;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A serializer for short arrays.
 */
@Internal
public final class ShortPrimitiveArraySerializer extends TypeSerializerSingleton<short[]>{

	private static final long serialVersionUID = 1L;
	
	private static final short[] EMPTY = new short[0];

	public static final ShortPrimitiveArraySerializer INSTANCE = new ShortPrimitiveArraySerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}
	
	@Override
	public short[] createInstance() {
		return EMPTY;
	}

	@Override
	public short[] copy(short[] from) {
		short[] copy = new short[from.length];
		System.arraycopy(from, 0, copy, 0, from.length);
		return copy;
	}
	
	@Override
	public short[] copy(short[] from, short[] reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	public void serialize(short[] record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		
		final int len = record.length;
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			target.writeShort(record[i]);
		}
	}

	@Override
	public short[] deserialize(DataInputView source) throws IOException {
		final int len = source.readInt();
		short[] array = new short[len];
		
		for (int i = 0; i < len; i++) {
			array[i] = source.readShort();
		}
		
		return array;
	}

	@Override
	public short[] deserialize(short[] reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int len = source.readInt();
		target.writeInt(len);
		target.write(source, len * 2);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ShortPrimitiveArraySerializer;
	}
}
