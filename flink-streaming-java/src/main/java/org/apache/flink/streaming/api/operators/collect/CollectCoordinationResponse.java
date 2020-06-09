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
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CoordinationResponse} from the coordinator containing the required batch or new results
 * and other necessary information in serialized form.
 *
 * <p>For an explanation of this communication protocol, see Java docs in {@link CollectSinkFunction}.
 */
public class CollectCoordinationResponse implements CoordinationResponse {

	private static final long serialVersionUID = 1L;

	private static final TypeSerializer<String> versionSerializer = StringSerializer.INSTANCE;
	private static final TypeSerializer<Long> offsetSerializer = LongSerializer.INSTANCE;
	private static final ListSerializer<byte[]> bufferSerializer =
		new ListSerializer<>(BytePrimitiveArraySerializer.INSTANCE);

	private final String version;
	private final long lastCheckpointedOffset;
	private final List<byte[]> serializedResults;

	public CollectCoordinationResponse(String version, long lastCheckpointedOffset, List<byte[]> serializedResults) {
		this.version = version;
		this.lastCheckpointedOffset = lastCheckpointedOffset;
		this.serializedResults = serializedResults;
	}

	public CollectCoordinationResponse(DataInputView inView) throws IOException {
		this.version = versionSerializer.deserialize(inView);
		this.lastCheckpointedOffset = offsetSerializer.deserialize(inView);
		this.serializedResults = bufferSerializer.deserialize(inView);
	}

	public String getVersion() {
		return version;
	}

	public long getLastCheckpointedOffset() {
		return lastCheckpointedOffset;
	}

	// TODO the following two methods might be not so efficient
	//  optimize them with MemorySegment if needed

	public <T> List<T> getResults(TypeSerializer<T> elementSerializer) throws IOException {
		List<T> results = new ArrayList<>();
		for (byte[] serializedResult : serializedResults) {
			ByteArrayInputStream bais = new ByteArrayInputStream(serializedResult);
			DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
			results.add(elementSerializer.deserialize(wrapper));
		}
		return results;
	}

	public void serialize(DataOutputView outView) throws IOException {
		versionSerializer.serialize(version, outView);
		offsetSerializer.serialize(lastCheckpointedOffset, outView);
		bufferSerializer.serialize(serializedResults, outView);
	}
}
