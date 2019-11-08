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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.Objects;

/**
 * Test for {@link org.apache.flink.table.runtime.typeutils.serializers.python.BaseRowSerializer}.
 */
public class BaseRowSerializerTest extends SerializerTestBase<BaseRow> {
	public BaseRowSerializerTest() {
		super(
			new DeeplyEqualsChecker()
				.withCustomCheck(
					(o1, o2) -> o1 instanceof BaseRow && o2 instanceof BaseRow,
					(o1, o2, checker) -> {
						LogicalType[] fieldTypes = new LogicalType[] {
							new BigIntType(),
							new BigIntType()
						};
						BaseRowSerializer serializer = new BaseRowSerializer(new ExecutionConfig(), fieldTypes);
						return deepEqualsBaseRow(
							(BaseRow) o1,
							(BaseRow) o2,
							(BaseRowSerializer) serializer.duplicate(),
							(BaseRowSerializer) serializer.duplicate());
					}
				));
	}

	@Override
	protected TypeSerializer<BaseRow> createSerializer() {
		TypeSerializer<?>[] fieldTypeSerializers = {
			LongSerializer.INSTANCE,
			LongSerializer.INSTANCE
		};

		LogicalType[] fieldTypes = {
			new BigIntType(),
			new BigIntType()
		};
		return new org.apache.flink.table.runtime.typeutils.serializers.python.BaseRowSerializer(
			fieldTypes,
			fieldTypeSerializers);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BaseRow> getTypeClass() {
		return BaseRow.class;
	}

	private static boolean deepEqualsBaseRow(
		BaseRow should, BaseRow is,
		BaseRowSerializer serializer1, BaseRowSerializer serializer2) {
		if (should.getArity() != is.getArity()) {
			return false;
		}
		BinaryRow row1 = serializer1.toBinaryRow(should);
		BinaryRow row2 = serializer2.toBinaryRow(is);

		return Objects.equals(row1, row2);
	}

	@Override
	protected BaseRow[] getTestData() {
		BaseRow row1 = StreamRecordUtils.baserow(null, 1L);
		BinaryRow row2 = StreamRecordUtils.binaryrow(1L, null);
		return new BaseRow[]{row1, row2};
	}
}
