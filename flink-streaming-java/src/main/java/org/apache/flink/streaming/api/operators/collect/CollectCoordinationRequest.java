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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import java.io.IOException;

/**
 * A {@link CoordinationRequest} from the client indicating that it wants a new batch of query results.
 *
 * <p>For an explanation of this communication protocol, see Java docs in {@link CollectSinkFunction}.
 */
public class CollectCoordinationRequest implements CoordinationRequest {

	private static final long serialVersionUID = 1L;

	private static final TypeSerializer<String> versionSerializer = StringSerializer.INSTANCE;
	private static final TypeSerializer<Long> offsetSerializer = LongSerializer.INSTANCE;

	private final String version;
	private final long offset;

	public CollectCoordinationRequest(String version, long offset) {
		this.version = version;
		this.offset = offset;
	}

	public CollectCoordinationRequest(DataInputView inView) throws IOException {
		this.version = versionSerializer.deserialize(inView);
		this.offset = offsetSerializer.deserialize(inView);
	}

	public String getVersion() {
		return version;
	}

	public long getOffset() {
		return offset;
	}

	public void serialize(DataOutputView outView) throws IOException {
		versionSerializer.serialize(version, outView);
		offsetSerializer.serialize(offset, outView);
	}
}
