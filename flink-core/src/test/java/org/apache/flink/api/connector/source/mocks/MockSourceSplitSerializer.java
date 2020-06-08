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

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * The serializer for MockSourceSplitSerializer.
 */
public class MockSourceSplitSerializer implements SimpleVersionedSerializer<MockSourceSplit> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(MockSourceSplit split) throws IOException {
		return InstantiationUtil.serializeObject(split);
	}

	@Override
	public MockSourceSplit deserialize(int version, byte[] serialized) throws IOException {
		try {
			return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to deserialize the split.", e);
		}
	}
}
