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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serializer/deserializer used for conversion between state and skip list value.
 * It is not thread safe.
 *
 * @param <S> type of state.
 */
class SkipListValueSerializer<S> {

	private final TypeSerializer<S> stateSerializer;
	/** The reusable output serialization buffer. */
	private DataOutputSerializer dos;
	/** The reusable input deserialization buffer. */
	private DataInputDeserializer dis;

	SkipListValueSerializer(TypeSerializer<S> stateSerializer) {
		this.stateSerializer = stateSerializer;
	}

	byte[] serialize(S state) {
		if (dos == null) {
			dos = new DataOutputSerializer(16);
		}
		try {
			stateSerializer.serialize(state, dos);
		} catch (IOException e) {
			throw new RuntimeException("serialize key and namespace failed", e);
		}
		byte[] ret = dos.getCopyOfBuffer();
		dos.clear();
		return ret;
	}

	/**
	 * Deserialize the state from the byte buffer which stores skip list value.
	 *
	 * @param byteBuffer the byte buffer which stores the skip list value.
	 * @param offset     the start position of the skip list value in the byte buffer.
	 * @param len        length of the skip list value.
	 */
	S deserializeState(ByteBuffer byteBuffer, int offset, int len) {
		if (dis != null) {
			dis.setBuffer(byteBuffer.array(), offset, len);
		} else {
			dis = new DataInputDeserializer(byteBuffer.array(), offset, len);
		}
		try {
			return stateSerializer.deserialize(dis);
		} catch (IOException e) {
			throw new RuntimeException("deserialize state failed", e);
		}
	}
}
