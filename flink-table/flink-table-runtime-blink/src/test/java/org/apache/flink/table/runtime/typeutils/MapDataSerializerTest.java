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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.data.util.MapDataUtil.convertToJavaMap;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObj;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObjSerializer;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.snapshotAndReconfigure;
import static org.junit.Assert.assertEquals;

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

	@Test
	public void testExecutionConfigWithKryo() throws Exception {
		// serialize base array
		ExecutionConfig config = new ExecutionConfig();
		config.enableForceKryo();
		config.registerTypeWithKryoSerializer(MyObj.class, new MyObjSerializer());
		final MapDataSerializer serializer = createSerializerWithConfig(config);

		int inputKey = 998244353;
		MyObj inputObj = new MyObj(114514, 1919810);
		Map<Object, Object> javaMap = new HashMap<>();
		javaMap.put(inputKey, RawValueData.fromObject(inputObj));
		MapData inputMap = new GenericMapData(javaMap);

		byte[] serialized;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			serializer.serialize(inputMap, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		// deserialize base array using restored serializer
		final MapDataSerializer restoreSerializer =
			(MapDataSerializer) snapshotAndReconfigure(serializer, () -> createSerializerWithConfig(config));

		MapData outputMap;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
			outputMap = restoreSerializer.deserialize(new DataInputViewStreamWrapper(in));
		}

		TypeSerializer restoreKeySer = restoreSerializer.getKeySerializer();
		TypeSerializer restoreValueSer = restoreSerializer.getValueSerializer();
		assertEquals(serializer.getKeySerializer(), restoreKeySer);
		assertEquals(serializer.getValueSerializer(), restoreValueSer);

		Map<Object, Object> outputJavaMap = convertToJavaMap(
			outputMap,
			DataTypes.INT().getLogicalType(),
			DataTypes.RAW(TypeInformation.of(MyObj.class)).getLogicalType());
		MyObj outputObj = ((RawValueData<MyObj>) outputJavaMap.get(inputKey))
			.toObject(new KryoSerializer<>(MyObj.class, config));
		assertEquals(inputObj, outputObj);
	}

	private MapDataSerializer createSerializerWithConfig(ExecutionConfig config) {
		return new MapDataSerializer(
			DataTypes.INT().getLogicalType(),
			DataTypes.RAW(TypeInformation.of(MyObj.class)).getLogicalType(),
			config);
	}

	private static MapDataSerializer newSer() {
		return new MapDataSerializer(INT, STRING, new ExecutionConfig());
	}

	@Override
	protected MapDataSerializer createSerializer() {
		return newSer();
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
}
