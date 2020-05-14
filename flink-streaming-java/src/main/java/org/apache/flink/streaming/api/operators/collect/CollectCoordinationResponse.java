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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * A {@link CoordinationResponse} from the coordinator containing the required batch or new results
 * and other necessary information in serialized form.
 */
public class CollectCoordinationResponse<T> implements CoordinationResponse {

	private static final long serialVersionUID = 1L;

	public static CollectCoordinationResponse INVALID_INSTANCE = new CollectCoordinationResponse();

	private static final TypeSerializer<String> versionSerializer = StringSerializer.INSTANCE;
	private static final TypeSerializer<Long> offsetSerializer = LongSerializer.INSTANCE;
	private static final TypeSerializer<Long> checkpointIdSerializer = LongSerializer.INSTANCE;

	private static final String INVALID_VERSION = "invalid version";

	private final String version;
	private final long offset;
	private final long lastCheckpointId;
	private final byte[] resultBytes;

	public CollectCoordinationResponse(
			String version,
			long offset,
			long lastCheckpointId,
			List<T> results,
			TypeSerializer<T> elementSerializer) throws IOException {
		this.version = version;
		this.offset = offset;
		this.lastCheckpointId = lastCheckpointId;

		ListSerializer<T> listSerializer = new ListSerializer<>(elementSerializer);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
		listSerializer.serialize(results, wrapper);
		this.resultBytes = baos.toByteArray();
	}

	public CollectCoordinationResponse(DataInputViewStreamWrapper wrapper) throws IOException {
		this.version = versionSerializer.deserialize(wrapper);
		this.offset = offsetSerializer.deserialize(wrapper);
		this.lastCheckpointId = offsetSerializer.deserialize(wrapper);

		int size = wrapper.readInt();
		this.resultBytes = new byte[size];
		wrapper.readFully(resultBytes);
	}

	public String getVersion() {
		return version;
	}

	public long getOffset() {
		return offset;
	}

	public long getLastCheckpointId() {
		return lastCheckpointId;
	}

	public List<T> getResults(TypeSerializer<T> elementSerializer) throws IOException {
		ListSerializer<T> listSerializer = new ListSerializer<>(elementSerializer);
		ByteArrayInputStream bais = new ByteArrayInputStream(resultBytes);
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
		return listSerializer.deserialize(wrapper);
	}

	public void serialize(DataOutputViewStreamWrapper wrapper) throws IOException {
		versionSerializer.serialize(version, wrapper);
		offsetSerializer.serialize(offset, wrapper);
		checkpointIdSerializer.serialize(lastCheckpointId, wrapper);

		wrapper.writeInt(resultBytes.length);
		wrapper.write(resultBytes);
	}

	private CollectCoordinationResponse() {
		this.version = INVALID_VERSION;
		this.offset = Long.MIN_VALUE;
		this.lastCheckpointId = Long.MIN_VALUE;
		this.resultBytes = null;
	}

	public static boolean isInvalid(CollectCoordinationResponse response) {
		return response.version.equals(INVALID_VERSION);
	}
}
