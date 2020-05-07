/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for the checkpoint of {@link MockSplitEnumerator}.
 */
public class MockSplitEnumeratorCheckpointSerializer implements SimpleVersionedSerializer<Set<MockSourceSplit>> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(Set<MockSourceSplit> obj) throws IOException {
		return InstantiationUtil.serializeObject(new ArrayList<>(obj));
	}

	@Override
	public Set<MockSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
		try {
			ArrayList<MockSourceSplit> list = InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
			return new HashSet<>(list);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException("Failed to deserialize the enumerator checkpoint.");
		}
	}

}
