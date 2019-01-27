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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serialization and deserialization schema that uses Flink's serialization stack to
 * transform typed from and to byte arrays.
 *
 * @param <T> The type to be serialized.
 */
@Public
public class TypeInformationSerializationSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {

	private static final long serialVersionUID = -5359448468131559102L;

	/** The type information, to be returned by {@link #getProducedType()}. */
	private final TypeInformation<T> typeInfo;

	/** The serializer for the actual de-/serialization. */
	private final TypeSerializer<T> serializer;

	/** The reusable output serialization buffer. */
	private transient DataOutputSerializer dos;

	/** The reusable input deserialization buffer. */
	private transient DataInputDeserializer dis;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new de-/serialization schema for the given type.
	 *
	 * @param typeInfo The type information for the type de-/serialized by this schema.
	 * @param ec The execution config, which is used to parametrize the type serializers.
	 */
	public TypeInformationSerializationSchema(TypeInformation<T> typeInfo, ExecutionConfig ec) {
		this.typeInfo = checkNotNull(typeInfo, "typeInfo");
		this.serializer = typeInfo.createSerializer(ec);
	}

	/**
	 * Creates a new de-/serialization schema for the given type.
	 *
	 * @param typeInfo The type information for the type de-/serialized by this schema.
	 * @param serializer The serializer to use for de-/serialization.
	 */
	public TypeInformationSerializationSchema(TypeInformation<T> typeInfo, TypeSerializer<T> serializer) {
		this.typeInfo = checkNotNull(typeInfo, "typeInfo");
		this.serializer = checkNotNull(serializer, "serializer");
	}

	// ------------------------------------------------------------------------

	@Override
	public T deserialize(byte[] message) {
		if (dis != null) {
			dis.setBuffer(message, 0, message.length);
		} else {
			dis = new DataInputDeserializer(message, 0, message.length);
		}

		try {
			return serializer.deserialize(dis);
		}
		catch (IOException e) {
			throw new RuntimeException("Unable to deserialize message", e);
		}
	}

	/**
	 * This schema never considers an element to signal end-of-stream, so this method returns always false.
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return Returns false.
	 */
	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(T element) {
		if (dos == null) {
			dos = new DataOutputSerializer(16);
		}

		try {
			serializer.serialize(element, dos);
		}
		catch (IOException e) {
			throw new RuntimeException("Unable to serialize record", e);
		}

		byte[] ret = dos.getCopyOfBuffer();
		dos.clear();
		return ret;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return typeInfo;
	}
}
