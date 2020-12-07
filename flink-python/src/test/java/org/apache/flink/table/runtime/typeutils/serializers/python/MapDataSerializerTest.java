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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.data.util.MapDataUtil.convertToJavaMap;

/**
 * Test for {@link MapDataSerializerTest}.
 */
public class MapDataSerializerTest extends SerializerTestBase<MapData> {

	private static final LogicalType BIGINT = DataTypes.BIGINT().getLogicalType();
	private static final LogicalType FLOAT = DataTypes.FLOAT().getLogicalType();

	public MapDataSerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
			(o1, o2) -> o1 instanceof MapData && o2 instanceof MapData,
			(o1, o2, checker) ->
				// Better is more proper to compare the maps after changing them to Java maps
				// instead of binary maps. For example, consider the following two maps:
				// {1: 1.0F, 2: 2.0F, 3: 3.0F} and {3: 3.0F, 2: 2.0F, 1: 1.0F}
				// These are actually the same maps, but their key / value order will be
				// different when stored as binary maps, and the equalsTo method of binary
				// map will return false.
				convertToJavaMap((MapData) o1, BIGINT, FLOAT)
					.equals(convertToJavaMap((MapData) o2, BIGINT, FLOAT))
		));
	}

	@Override
	protected TypeSerializer<MapData> createSerializer() {
		return new MapDataSerializer(
			new BigIntType(), new FloatType(), LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<MapData> getTypeClass() {
		return MapData.class;
	}

	@Override
	protected MapData[] getTestData() {
		Map<Object, Object> first = new HashMap<>();
		first.put(1L, -100.1F);
		BinaryArrayData keyBinary = BinaryArrayData.fromPrimitiveArray(new long[]{10L});
		BinaryArrayData valueBinary = BinaryArrayData.fromPrimitiveArray(new float[]{10.2F});
		return new MapData[]{new GenericMapData(first), BinaryMapData.valueOf(keyBinary, valueBinary)};
	}
}
