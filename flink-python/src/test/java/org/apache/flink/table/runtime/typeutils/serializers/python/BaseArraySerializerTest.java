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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;

/**
 * Test for {@link BaseArraySerializer}.
 */
public class BaseArraySerializerTest {

	/**
	 * Test for BaseArray with Primitive data type.
	 */
	public static class BaseArrayWithPrimitiveTest extends SerializerTestBase<BaseArray> {
		@Override
		protected TypeSerializer<BaseArray> createSerializer() {
			return new BaseArraySerializer(new BigIntType(), LongSerializer.INSTANCE);
		}

		@Override
		protected int getLength() {
			return -1;
		}

		@Override
		protected Class<BaseArray> getTypeClass() {
			return BaseArray.class;
		}

		@Override
		protected BaseArray[] getTestData() {
			return new BinaryArray[]{BinaryArray.fromPrimitiveArray(new long[]{100L})};
		}
	}

	/**
	 * Test for BaseArray with BaseArray data type.
	 */
	public static class BaseArrayWithBinaryArrayTest extends SerializerTestBase<BaseArray> {

		@Override
		protected TypeSerializer<BaseArray> createSerializer() {
			return new BaseArraySerializer(new ArrayType(new BigIntType()),
				new BaseArraySerializer(new BigIntType(), LongSerializer.INSTANCE));
		}

		@Override
		protected int getLength() {
			return -1;
		}

		@Override
		protected Class<BaseArray> getTypeClass() {
			return BaseArray.class;
		}

		@Override
		protected BaseArray[] getTestData() {
			BinaryArray elementArray = BinaryArray.fromPrimitiveArray(new long[]{100L});
			BaseArraySerializer elementTypeSerializer =
				new BaseArraySerializer(new BigIntType(), LongSerializer.INSTANCE);
			BinaryArray array = new BinaryArray();
			BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);
			writer.writeArray(0, elementArray, elementTypeSerializer);
			writer.complete();
			return new BinaryArray[]{array};
		}
	}
}
