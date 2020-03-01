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
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.GenericMap;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for {@link BaseMapSerializerTest}.
 */
public class BaseMapSerializerTest extends SerializerTestBase<BaseMap> {

	private static final LogicalType BIGINT = DataTypes.BIGINT().getLogicalType();
	private static final LogicalType FLOAT = DataTypes.FLOAT().getLogicalType();

	public BaseMapSerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
			(o1, o2) -> o1 instanceof BaseMap && o2 instanceof BaseMap,
			(o1, o2, checker) ->
				// Better is more proper to compare the maps after changing them to Java maps
				// instead of binary maps. For example, consider the following two maps:
				// {1: 1.0F, 2: 2.0F, 3: 3.0F} and {3: 3.0F, 2: 2.0F, 1: 1.0F}
				// These are actually the same maps, but their key / value order will be
				// different when stored as binary maps, and the equalsTo method of binary
				// map will return false.
				((BaseMap) o1).toJavaMap(BIGINT, FLOAT)
					.equals(((BaseMap) o2).toJavaMap(BIGINT, FLOAT))
		));
	}

	@Override
	protected TypeSerializer<BaseMap> createSerializer() {
		return new BaseMapSerializer(
			new BigIntType(), new FloatType(), LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<BaseMap> getTypeClass() {
		return BaseMap.class;
	}

	@Override
	protected BaseMap[] getTestData() {
		Map<Object, Object> first = new HashMap<>();
		first.put(1L, -100.1F);
		BinaryArray keyBinary = BinaryArray.fromPrimitiveArray(new long[]{10L});
		BinaryArray valueBinary = BinaryArray.fromPrimitiveArray(new float[]{10.2F});
		return new BaseMap[]{new GenericMap(first), BinaryMap.valueOf(keyBinary, valueBinary)};
	}
}
