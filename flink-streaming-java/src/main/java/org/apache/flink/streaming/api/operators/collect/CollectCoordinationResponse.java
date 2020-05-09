/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CoordinationResponse} from the coordinator containing the required batch or new results
 * and other necessary information in serialized form.
 */
public class CollectCoordinationResponse implements CoordinationResponse {

	private static final long serialVersionUID = 1L;

	private final byte[] bytes;

	public CollectCoordinationResponse(byte[] bytes) {
		this.bytes = bytes;
	}

	public <T> DeserializedResponse<T> getDeserializedResponse(TypeSerializer<T> serializer) {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
		TypeSerializer<String> versionSerializer = new StringSerializer();
		TypeSerializer<Long> tokenSerializer = new LongSerializer();
		try {
			String version = versionSerializer.deserialize(wrapper);
			long token = tokenSerializer.deserialize(wrapper);
			long checkpointedToken = tokenSerializer.deserialize(wrapper);
			int length = wrapper.readInt();
			List<T> results = new ArrayList<>(length);
			for (int i = 0; i < length; i++) {
				results.add(serializer.deserialize(wrapper));
			}
			return new DeserializedResponse<>(version, token, checkpointedToken, results);
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize collect sink result", e);
		}
	}

	public static <T> byte[] serialize(
			String version,
			long token,
			long lastCheckpointId,
			List<T> results,
			TypeSerializer<T> serializer) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
		TypeSerializer<String> versionSerializer = new StringSerializer();
		TypeSerializer<Long> tokenSerializer = new LongSerializer();
		try {
			versionSerializer.serialize(version, wrapper);
			tokenSerializer.serialize(token, wrapper);
			tokenSerializer.serialize(lastCheckpointId, wrapper);
			wrapper.writeInt(results.size());
			for (T result : results) {
				serializer.serialize(result, wrapper);
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize collect sink result", e);
		}
		return baos.toByteArray();
	}

	/**
	 * Deserialized response containing version, token and result batch.
	 *
	 * @param <T> type of result
	 */
	public static class DeserializedResponse<T> {

		private final String version;
		private final long token;
		private final long lastCheckpointId;
		private final List<T> results;

		private DeserializedResponse(String version, long token, long lastCheckpointId, List<T> results) {
			this.version = version;
			this.token = token;
			this.lastCheckpointId = lastCheckpointId;
			this.results = results;
		}

		public String getVersion() {
			return version;
		}

		public long getToken() {
			return token;
		}

		public long getLastCheckpointId() {
			return lastCheckpointId;
		}

		public List<T> getResults() {
			return results;
		}
	}
}
