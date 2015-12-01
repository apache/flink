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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;

import java.io.IOException;

/**
 * A serialization and deserialization schema that uses Flink's serialization stack to
 * transform typed from and to byte arrays.
 * 
 * @param <T> The type to be serialized.
 */
@Public
public class TypeInformationSerializationSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {
	
	private static final long serialVersionUID = -5359448468131559102L;
	
	/** The serializer for the actual de-/serialization */
	private final TypeSerializer<T> serializer;

	/** The reusable output serialization buffer */
	private transient DataOutputSerializer dos;
	
	/** The reusable input deserialization buffer */
	private transient DataInputDeserializer dis;

	/** The type information, to be returned by {@link #getProducedType()}. It is
	 * transient, because it is not serializable. Note that this means that the type information
	 * is not available at runtime, but only prior to the first serialization / deserialization */
	private transient TypeInformation<T> typeInfo;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new de-/serialization schema for the given type.
	 * 
	 * @param typeInfo The type information for the type de-/serialized by this schema.
	 * @param ec The execution config, which is used to parametrize the type serializers.
	 */
	public TypeInformationSerializationSchema(TypeInformation<T> typeInfo, ExecutionConfig ec) {
		this.typeInfo = typeInfo;
		this.serializer = typeInfo.createSerializer(ec);
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
		
		byte[] ret = dos.getByteArray();
		if (ret.length != dos.length()) {
			byte[] n = new byte[dos.length()];
			System.arraycopy(ret, 0, n, 0, dos.length());
			ret = n;
		}
		dos.clear();
		return ret;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		if (typeInfo != null) {
			return typeInfo;
		}
		else {
			throw new IllegalStateException(
					"The type information is not available after this class has been serialized and distributed.");
		}
	}
}
