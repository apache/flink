/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SimpleVersionedSerializerTypeSerializerProxy}.
 */
public class SimpleVersionedSerializerTypeSerializerProxyTest extends SerializerTestBase<String> {

	@Override
	protected TypeSerializer<String> createSerializer() {
		return new SimpleVersionedSerializerTypeSerializerProxy<>(TestStringSerializer::new);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<String> getTypeClass() {
		return String.class;
	}

	@Override
	protected String[] getTestData() {
		return new String[]{"a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"};
	}

	@Override
	public void testInstantiate() {
		// this serializer does not support instantiation
	}

	@Override
	public void testConfigSnapshotInstantiation() {
		// this serializer does not support snapshots
	}

	@Override
	public void testSnapshotConfigurationAndReconfigure() {
		// this serializer does not support snapshots
	}

	private static final class TestStringSerializer implements SimpleVersionedSerializer<String> {

		private static final int VERSION = 1;

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public byte[] serialize(String str) {
			return str.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public String deserialize(int version, byte[] serialized) {
			assertEquals(VERSION, version);
			return new String(serialized, StandardCharsets.UTF_8);
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof TestStringSerializer;
		}
	}

}
