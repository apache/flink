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
import org.apache.flink.table.dataformat.BinaryString;

/**
 * A test for the {@link BinaryArraySerializer}.
 */
public class BinaryArraySerializerTest extends SerializerTestBase<BinaryArray> {

	@Override
	protected BinaryArraySerializer createSerializer() {
		return BinaryArraySerializer.INSTANCE;
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BinaryArray> getTypeClass() {
		return BinaryArray.class;
	}

	@Override
	protected BinaryArray[] getTestData() {
		return new BinaryArray[] {
				createArray("11"),
				createArray("11", "haa"),
				createArray("11", "haa", "ke"),
				createArray("11", "haa", "ke"),
				createArray("11", "lele", "haa", "ke"),
		};
	}

	static BinaryArray createArray(String... vs) {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 8);
		for (int i = 0; i < vs.length; i++) {
			writer.writeString(i, BinaryString.fromString(vs[i]));
		}
		writer.complete();
		return array;
	}
}
