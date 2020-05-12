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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObj;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObjSerializer;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.snapshotAndReconfigure;
import static org.junit.Assert.assertEquals;

/**
 * A test for the {@link ArrayDataSerializer}.
 */
public class ArrayDataSerializerTest extends SerializerTestBase<ArrayData> {

	public ArrayDataSerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
				(o1, o2) -> o1 instanceof ArrayData && o2 instanceof ArrayData,
				(o1, o2, checker) -> {
					ArrayData array1 = (ArrayData) o1;
					ArrayData array2 = (ArrayData) o2;
					if (array1.size() != array2.size()) {
						return false;
					}
					for (int i = 0; i < array1.size(); i++) {
						if (!array1.isNullAt(i) || !array2.isNullAt(i)) {
							if (array1.isNullAt(i) || array2.isNullAt(i)) {
								return false;
							} else {
								if (!array1.getString(i).equals(array2.getString(i))) {
									return false;
								}
							}
						}
					}
					return true;
				}
		));
	}

	@Test
	public void testExecutionConfigWithKryo() throws Exception {
		// serialize base array
		ExecutionConfig config = new ExecutionConfig();
		config.enableForceKryo();
		config.registerTypeWithKryoSerializer(MyObj.class, new MyObjSerializer());
		final ArrayDataSerializer serializer = createSerializerWithConfig(config);

		MyObj inputObj = new MyObj(114514, 1919810);
		ArrayData inputArray = new GenericArrayData(new RawValueData[] {
			RawValueData.fromObject(inputObj)
		});

		byte[] serialized;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			serializer.serialize(inputArray, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		// deserialize base array using restored serializer
		final ArrayDataSerializer restoreSerializer =
			(ArrayDataSerializer) snapshotAndReconfigure(serializer, () -> createSerializerWithConfig(config));

		ArrayData outputArray;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
			outputArray = restoreSerializer.deserialize(new DataInputViewStreamWrapper(in));
		}

		TypeSerializer restoreEleSer = restoreSerializer.getEleSer();
		assertEquals(serializer.getEleSer(), restoreEleSer);

		MyObj outputObj = outputArray
			.<MyObj>getRawValue(0)
			.toObject(new KryoSerializer<>(MyObj.class, config));
		assertEquals(inputObj, outputObj);
	}

	private ArrayDataSerializer createSerializerWithConfig(ExecutionConfig config) {
		return new ArrayDataSerializer(
			DataTypes.RAW(TypeInformation.of(MyObj.class)).getLogicalType(), config);
	}

	@Override
	protected ArrayDataSerializer createSerializer() {
		return new ArrayDataSerializer(DataTypes.STRING().getLogicalType(), new ExecutionConfig());
	}

	@Override
	protected int getLength() {
		return -1;
	}

	@Override
	protected Class<ArrayData> getTypeClass() {
		return ArrayData.class;
	}

	@Override
	protected ArrayData[] getTestData() {
		return new ArrayData[] {
				new GenericArrayData(new StringData[] {StringData.fromString("11")}),
				createArray("11", "haa"),
				createArray("11", "haa", "ke"),
				createArray("11", "haa", "ke"),
				createArray("11", "lele", "haa", "ke"),
				createColumnarArray("11", "lele", "haa", "ke"),
		};
	}

	static BinaryArrayData createArray(String... vs) {
		BinaryArrayData array = new BinaryArrayData();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 8);
		for (int i = 0; i < vs.length; i++) {
			writer.writeString(i, StringData.fromString(vs[i]));
		}
		writer.complete();
		return array;
	}

	private static ColumnarArrayData createColumnarArray(String... vs) {
		HeapBytesVector vector = new HeapBytesVector(vs.length);
		for (String v : vs) {
			vector.fill(v.getBytes(StandardCharsets.UTF_8));
		}
		return new ColumnarArrayData(vector, 0, vs.length);
	}
}
