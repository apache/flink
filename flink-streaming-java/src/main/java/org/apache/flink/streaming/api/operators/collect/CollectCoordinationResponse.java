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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * A {@link CoordinationResponse} from the coordinator containing the required batch or new results
 * and other necessary information in serialized form.
 *
 * <p>For an explanation of this communication protocol, see Java docs in {@link CollectSinkFunction}.
 */
public class CollectCoordinationResponse<T> implements CoordinationResponse {

	private static final long serialVersionUID = 1L;

	private static final TypeSerializer<String> versionSerializer = StringSerializer.INSTANCE;
	private static final TypeSerializer<Long> offsetSerializer = LongSerializer.INSTANCE;

	private final String version;
	private final long lastCheckpointedOffset;
	private final byte[] resultBytes;

	public CollectCoordinationResponse(
			String version,
			long lastCheckpointedOffset,
			List<T> results,
			TypeSerializer<T> elementSerializer) throws IOException {
		this.version = version;
		this.lastCheckpointedOffset = lastCheckpointedOffset;

		ListSerializer<T> listSerializer = new ListSerializer<>(elementSerializer);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
		listSerializer.serialize(results, wrapper);
		this.resultBytes = baos.toByteArray();
	}

	public CollectCoordinationResponse(DataInputView inView) throws IOException {
		this.version = versionSerializer.deserialize(inView);
		this.lastCheckpointedOffset = offsetSerializer.deserialize(inView);

		int size = inView.readInt();
		this.resultBytes = new byte[size];
		inView.readFully(resultBytes);
	}

	public String getVersion() {
		return version;
	}

	public long getLastCheckpointedOffset() {
		return lastCheckpointedOffset;
	}

	// TODO the following two methods might be not so efficient
	//  optimize them with MemorySegment if needed

	public List<T> getResults(TypeSerializer<T> elementSerializer) throws IOException {
		ListSerializer<T> listSerializer = new ListSerializer<>(elementSerializer);
		ByteArrayInputStream bais = new ByteArrayInputStream(resultBytes);
		DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
		return listSerializer.deserialize(wrapper);
	}

	public void serialize(DataOutputView outView) throws IOException {
		versionSerializer.serialize(version, outView);
		offsetSerializer.serialize(lastCheckpointedOffset, outView);

		outView.writeInt(resultBytes.length);
		outView.write(resultBytes);
	}
}
