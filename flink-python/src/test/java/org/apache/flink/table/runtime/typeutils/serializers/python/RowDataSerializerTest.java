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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.Objects;

/**
 * Test for {@link org.apache.flink.table.runtime.typeutils.serializers.python.RowDataSerializer}.
 */
public class RowDataSerializerTest extends SerializerTestBase<RowData> {
	public RowDataSerializerTest() {
		super(
			new DeeplyEqualsChecker()
				.withCustomCheck(
					(o1, o2) -> o1 instanceof RowData && o2 instanceof RowData,
					(o1, o2, checker) -> {
						LogicalType[] fieldTypes = new LogicalType[] {
							new BigIntType(),
							new BigIntType()
						};
						RowDataSerializer serializer = new RowDataSerializer(fieldTypes);
						return deepEqualsRowData(
							(RowData) o1,
							(RowData) o2,
							(RowDataSerializer) serializer.duplicate(),
							(RowDataSerializer) serializer.duplicate());
					}
				));
	}

	@Override
	protected TypeSerializer<RowData> createSerializer() {
		TypeSerializer<?>[] fieldTypeSerializers = {
			LongSerializer.INSTANCE,
			LongSerializer.INSTANCE
		};

		LogicalType[] fieldTypes = {
			new BigIntType(),
			new BigIntType()
		};
		return new org.apache.flink.table.runtime.typeutils.serializers.python.RowDataSerializer(
			fieldTypes,
			fieldTypeSerializers);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<RowData> getTypeClass() {
		return RowData.class;
	}

	private static boolean deepEqualsRowData(
		RowData should, RowData is,
		RowDataSerializer serializer1, RowDataSerializer serializer2) {
		if (should.getArity() != is.getArity()) {
			return false;
		}
		BinaryRowData row1 = serializer1.toBinaryRow(should);
		BinaryRowData row2 = serializer2.toBinaryRow(is);

		return Objects.equals(row1, row2);
	}

	@Override
	protected RowData[] getTestData() {
		RowData row1 = StreamRecordUtils.row(null, 1L);
		BinaryRowData row2 = StreamRecordUtils.binaryrow(1L, null);
		return new RowData[]{row1, row2};
	}
}
