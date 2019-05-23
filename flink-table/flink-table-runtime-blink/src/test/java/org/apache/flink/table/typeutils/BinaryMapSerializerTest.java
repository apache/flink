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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;

/**
 * A test for the {@link BinaryArraySerializer}.
 */
public class BinaryMapSerializerTest extends SerializerTestBase<BinaryMap> {

	@Override
	protected BinaryMapSerializer createSerializer() {
		return BinaryMapSerializer.INSTANCE;
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BinaryMap> getTypeClass() {
		return BinaryMap.class;
	}

	@Override
	protected BinaryMap[] getTestData() {
		return new BinaryMap[] {
				BinaryMap.valueOf(createArray(1), BinaryArraySerializerTest.createArray("11")),
				BinaryMap.valueOf(createArray(1, 2), BinaryArraySerializerTest.createArray("11", "haa")),
				BinaryMap.valueOf(createArray(1, 3, 4), BinaryArraySerializerTest.createArray("11", "haa", "ke")),
				BinaryMap.valueOf(createArray(1, 4, 2), BinaryArraySerializerTest.createArray("11", "haa", "ke")),
				BinaryMap.valueOf(createArray(1, 5, 6, 7), BinaryArraySerializerTest.createArray("11", "lele", "haa", "ke"))
		};
	}

	private static BinaryArray createArray(int... vs) {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 8);
		for (int i = 0; i < vs.length; i++) {
			writer.writeInt(i, vs[i]);
		}
		writer.complete();
		return array;
	}
}
