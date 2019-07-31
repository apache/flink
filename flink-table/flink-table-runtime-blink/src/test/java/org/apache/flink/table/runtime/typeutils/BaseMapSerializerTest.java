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
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericMap;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObj;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObjSerializer;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.snapshotAndReconfigure;
import static org.junit.Assert.assertEquals;

/**
 * A test for the {@link BaseMapSerializer}.
 */
public class BaseMapSerializerTest extends SerializerTestBase<BaseMap> {

	private static final LogicalType INT = DataTypes.INT().getLogicalType();
	private static final LogicalType STRING = DataTypes.STRING().getLogicalType();

	public BaseMapSerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
				(o1, o2) -> o1 instanceof BaseMap && o2 instanceof BaseMap,
				(o1, o2, checker) ->
						// Better is more proper to compare the maps after changing them to Java maps
						// instead of binary maps. For example, consider the following two maps:
						// {1: 'a', 2: 'b', 3: 'c'} and {3: 'c', 2: 'b', 1: 'a'}
						// These are actually the same maps, but their key / value order will be
						// different when stored as binary maps, and the equalsTo method of binary
						// map will return false.
						((BaseMap) o1).toJavaMap(INT, STRING)
								.equals(((BaseMap) o2).toJavaMap(INT, STRING))
		));
	}

	@Test
	public void testExecutionConfigWithKryo() throws Exception {
		// serialize base array
		ExecutionConfig config = new ExecutionConfig();
		config.enableForceKryo();
		config.registerTypeWithKryoSerializer(MyObj.class, new MyObjSerializer());
		final BaseMapSerializer serializer = createSerializerWithConfig(config);

		int inputKey = 998244353;
		MyObj inputObj = new MyObj(114514, 1919810);
		Map<Object, Object> javaMap = new HashMap<>();
		javaMap.put(inputKey, new BinaryGeneric<>(inputObj, new KryoSerializer<>(MyObj.class, config)));
		BaseMap inputMap = new GenericMap(javaMap);

		byte[] serialized;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			serializer.serialize(inputMap, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		// deserialize base array using restored serializer
		final BaseMapSerializer restoreSerializer =
			(BaseMapSerializer) snapshotAndReconfigure(serializer, () -> createSerializerWithConfig(config));

		BaseMap outputMap;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
			outputMap = restoreSerializer.deserialize(new DataInputViewStreamWrapper(in));
		}

		TypeSerializer restoreKeySer = restoreSerializer.getKeySerializer();
		TypeSerializer restoreValueSer = restoreSerializer.getValueSerializer();
		assertEquals(serializer.getKeySerializer(), restoreKeySer);
		assertEquals(serializer.getValueSerializer(), restoreValueSer);

		MyObj outputObj = BinaryGeneric.getJavaObjectFromBinaryGeneric(
			(BinaryGeneric) outputMap.toJavaMap(
				DataTypes.INT().getLogicalType(), DataTypes.ANY(TypeInformation.of(MyObj.class)).getLogicalType())
				.get(inputKey), new KryoSerializer<>(MyObj.class, config));
		assertEquals(inputObj, outputObj);
	}

	private BaseMapSerializer createSerializerWithConfig(ExecutionConfig config) {
		return new BaseMapSerializer(
			DataTypes.INT().getLogicalType(),
			DataTypes.ANY(TypeInformation.of(MyObj.class)).getLogicalType(),
			config);
	}

	private static BaseMapSerializer newSer() {
		return new BaseMapSerializer(INT, STRING, new ExecutionConfig());
	}

	@Override
	protected BaseMapSerializer createSerializer() {
		return newSer();
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
		first.put(1, BinaryString.fromString(""));
		return new BaseMap[] {
				new GenericMap(first),
				BinaryMap.valueOf(createArray(1, 2), BaseArraySerializerTest.createArray("11", "haa")),
				BinaryMap.valueOf(createArray(1, 3, 4), BaseArraySerializerTest.createArray("11", "haa", "ke")),
				BinaryMap.valueOf(createArray(1, 4, 2), BaseArraySerializerTest.createArray("11", "haa", "ke")),
				BinaryMap.valueOf(createArray(1, 5, 6, 7), BaseArraySerializerTest.createArray("11", "lele", "haa", "ke"))
		};
	}

	private static BinaryArray createArray(int... vs) {
		BinaryArray array = new BinaryArray();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 4);
		for (int i = 0; i < vs.length; i++) {
			writer.writeInt(i, vs[i]);
		}
		writer.complete();
		return array;
	}
}
