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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.data.util.MapDataUtil.convertToJavaMap;

/**
 * A test for the {@link MapDataSerializer}.
 */
public class MapDataSerializerTest extends SerializerTestBase<MapData> {

	private static final LogicalType INT = DataTypes.INT().getLogicalType();
	private static final LogicalType STRING = DataTypes.STRING().getLogicalType();

	public MapDataSerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
				(o1, o2) -> o1 instanceof MapData && o2 instanceof MapData,
				(o1, o2, checker) ->
						// Better is more proper to compare the maps after changing them to Java maps
						// instead of binary maps. For example, consider the following two maps:
						// {1: 'a', 2: 'b', 3: 'c'} and {3: 'c', 2: 'b', 1: 'a'}
						// These are actually the same maps, but their key / value order will be
						// different when stored as binary maps, and the equalsTo method of binary
						// map will return false.
						convertToJavaMap((MapData) o1, INT, STRING)
							.equals(convertToJavaMap((MapData) o2, INT, STRING))
		));
	}

	@Override
	protected MapDataSerializer createSerializer() {
		return new MapDataSerializer(INT, STRING);
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
		first.put(1, StringData.fromString(""));
		return new MapData[] {
				new GenericMapData(first),
				BinaryMapData.valueOf(createArray(1, 2), ArrayDataSerializerTest.createArray("11", "haa")),
				BinaryMapData.valueOf(createArray(1, 3, 4), ArrayDataSerializerTest.createArray("11", "haa", "ke")),
				BinaryMapData.valueOf(createArray(1, 4, 2), ArrayDataSerializerTest.createArray("11", "haa", "ke")),
				BinaryMapData.valueOf(createArray(1, 5, 6, 7), ArrayDataSerializerTest.createArray("11", "lele", "haa", "ke"))
		};
	}

	private static BinaryArrayData createArray(int... vs) {
		BinaryArrayData array = new BinaryArrayData();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 4);
		for (int i = 0; i < vs.length; i++) {
			writer.writeInt(i, vs[i]);
		}
		writer.complete();
		return array;
	}

	@Test
	public void testToBinaryMapWithCompactDecimal() {
		testToBinaryMapWithDecimal(4);
	}

	@Test
	public void testToBinaryMapWithNotCompactDecimal() {
		testToBinaryMapWithDecimal(38);
	}

	private void testToBinaryMapWithDecimal(int precision) {
		DecimalData decimal = DecimalData.fromBigDecimal(new BigDecimal(123), precision, 0);

		BinaryArrayData expectedKeys = new BinaryArrayData();
		BinaryArrayWriter keyWriter = new BinaryArrayWriter(expectedKeys, 1, 8);
		keyWriter.writeDecimal(0, decimal, precision);
		keyWriter.complete();
		BinaryArrayData expectedValues = new BinaryArrayData();
		BinaryArrayWriter valueWriter = new BinaryArrayWriter(expectedValues, 1, 8);
		valueWriter.writeNullDecimal(0, precision);
		valueWriter.complete();
		BinaryMapData expected = BinaryMapData.valueOf(expectedKeys, expectedValues);

		MapDataSerializer serializer = new MapDataSerializer(
			new DecimalType(precision, 0),
			new DecimalType(precision, 0));
		GenericMapData genericMap = new GenericMapData(Collections.singletonMap(decimal, null));
		BinaryMapData actual = serializer.toBinaryMap(genericMap);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testToBinaryMapWithCompactTimestamp() {
		testToBinaryMapWithTimestamp(3);
	}

	@Test
	public void testToBinaryMapWithNotCompactTimestamp() {
		testToBinaryMapWithTimestamp(9);
	}

	private void testToBinaryMapWithTimestamp(int precision) {
		TimestampData timestamp = TimestampData.fromTimestamp(new Timestamp(123));

		BinaryArrayData expectedKeys = new BinaryArrayData();
		BinaryArrayWriter keyWriter = new BinaryArrayWriter(expectedKeys, 1, 8);
		keyWriter.writeTimestamp(0, timestamp, precision);
		keyWriter.complete();
		BinaryArrayData expectedValues = new BinaryArrayData();
		BinaryArrayWriter valueWriter = new BinaryArrayWriter(expectedValues, 1, 8);
		valueWriter.writeNullTimestamp(0, precision);
		valueWriter.complete();
		BinaryMapData expected = BinaryMapData.valueOf(expectedKeys, expectedValues);

		MapDataSerializer serializer = new MapDataSerializer(
			new TimestampType(precision),
			new TimestampType(precision));
		GenericMapData genericMap = new GenericMapData(Collections.singletonMap(timestamp, null));
		BinaryMapData actual = serializer.toBinaryMap(genericMap);

		Assert.assertEquals(expected, actual);
	}
}
