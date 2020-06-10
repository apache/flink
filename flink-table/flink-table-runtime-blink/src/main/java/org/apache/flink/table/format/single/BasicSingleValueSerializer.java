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

package org.apache.flink.table.format.single;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Single value serializer for java basic value.
 */
public class BasicSingleValueSerializer<T> extends SingleValueSerializer<T> {

	private TypeSerializerSingleton<T> typeSerializer;
	private DataInputDeserializer dataInputDeserializer;
	private DataOutputSerializer dataOutputSerializer;
	private TypeInformation<T> typeInfo;

	public BasicSingleValueSerializer(TypeSerializerSingleton<T> typeSerializer, TypeInformation<T> typeInfo) {
		this.typeSerializer = typeSerializer;
		this.dataInputDeserializer = new DataInputDeserializer();
		this.dataOutputSerializer = new DataOutputSerializer(1);
		this.typeInfo = typeInfo;
	}

	@Override
	public T deserialize(byte[] message) throws IOException {
		dataInputDeserializer.setBuffer(message);
		return typeSerializer.deserialize(dataInputDeserializer);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(T element) {
		try {
			this.typeSerializer.serialize(element, dataOutputSerializer);
			byte[] buffer = dataOutputSerializer.getCopyOfBuffer();
			dataOutputSerializer.clear();
			return buffer;
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize ", e);
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return typeInfo;
	}
}
