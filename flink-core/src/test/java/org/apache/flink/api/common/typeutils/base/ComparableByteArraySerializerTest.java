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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.junit.Ignore;

import java.util.concurrent.ThreadLocalRandom;

public class ComparableByteArraySerializerTest extends SerializerTestBase<byte[]> {

	@Override
	protected TypeSerializer<byte[]> createSerializer() {
		return BytewiseComparator.BYTEARRAY_INSTANCE.getSerializer();
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<byte[]> getTypeClass() {
		return byte[].class;
	}

	@Override
	protected byte[][] getTestData() {
		return new byte[][] {
			randomByteArray(),
			randomByteArray(),
			new byte[] {},
			randomByteArray(),
			randomByteArray(),
			randomByteArray(),
			new byte[] {},
			randomByteArray(),
			randomByteArray(),
			randomByteArray(),
			new byte[] {}
		};
	}

	private final byte[] randomByteArray() {
		int len = ThreadLocalRandom.current().nextInt(1024 * 1024);
		byte[] data = new byte[len];
		for (int i = 0; i < len; i++) {
			data[i] = (byte) ThreadLocalRandom.current().nextInt();
		}
		return data;
	}

	// Byte wise serializer does not have any information about length,
	// thus any tests de/serialize objects sequentially should be ignored.
	@Override
	@Ignore
	public void testSerializeAsSequenceNoReuse() {
	}

	@Override
	@Ignore
	public void testSerializeAsSequenceReusingValues() {
	}

	@Override
	@Ignore
	public void testSerializedCopyAsSequence() {
	}
}
