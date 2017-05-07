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
import org.apache.flink.types.StringValue;


/**
 * A serializer for String arrays. Specialized for efficiency.
 */
@Internal
public final class StringArraySerializer extends TypeSerializerSingleton<String[]>{

	private static final long serialVersionUID = 1L;
	
	private static final String[] EMPTY = new String[0];
	
	public static final StringArraySerializer INSTANCE = new StringArraySerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public String[] createInstance() {
		return EMPTY;
	}

	@Override
	public String[] copy(String[] from) {
		String[] target = new String[from.length];
		System.arraycopy(from, 0, target, 0, from.length);
		return target;
	}
	
	@Override
	public String[] copy(String[] from, String[] reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(String[] record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		
		final int len = record.length;
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			StringValue.writeString(record[i], target);
		}
	}

	@Override
	public String[] deserialize(DataInputView source) throws IOException {
		final int len = source.readInt();
		String[] array = new String[len];
		
		for (int i = 0; i < len; i++) {
			array[i] = StringValue.readString(source);
		}
		
		return array;
	}
	
	@Override
	public String[] deserialize(String[] reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int len = source.readInt();
		target.writeInt(len);
		
		for (int i = 0; i < len; i++) {
			StringValue.copyString(source, target);
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StringArraySerializer;
	}
}
