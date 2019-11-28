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
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericArray;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObj;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.MyObjSerializer;
import static org.apache.flink.table.runtime.typeutils.SerializerTestUtil.snapshotAndReconfigure;
import static org.junit.Assert.assertEquals;

/**
 * A test for the {@link BaseArraySerializer}.
 */
public class BaseArraySerializerTest extends SerializerTestBase<BaseArray> {

	public BaseArraySerializerTest() {
		super(new DeeplyEqualsChecker().withCustomCheck(
				(o1, o2) -> o1 instanceof BaseArray && o2 instanceof BaseArray,
				(o1, o2, checker) -> {
					BaseArray array1 = (BaseArray) o1;
					BaseArray array2 = (BaseArray) o2;
					if (array1.numElements() != array2.numElements()) {
						return false;
					}
					for (int i = 0; i < array1.numElements(); i++) {
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
		final BaseArraySerializer serializer = createSerializerWithConfig(config);

		MyObj inputObj = new MyObj(114514, 1919810);
		BaseArray inputArray = new GenericArray(new BinaryGeneric[] {
			new BinaryGeneric<>(inputObj)
		}, 1);

		byte[] serialized;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			serializer.serialize(inputArray, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		// deserialize base array using restored serializer
		final BaseArraySerializer restoreSerializer =
			(BaseArraySerializer) snapshotAndReconfigure(serializer, () -> createSerializerWithConfig(config));

		BaseArray outputArray;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serialized)) {
			outputArray = restoreSerializer.deserialize(new DataInputViewStreamWrapper(in));
		}

		TypeSerializer restoreEleSer = restoreSerializer.getEleSer();
		assertEquals(serializer.getEleSer(), restoreEleSer);

		MyObj outputObj = BinaryGeneric.getJavaObjectFromBinaryGeneric(
			outputArray.getGeneric(0), new KryoSerializer<>(MyObj.class, config));
		assertEquals(inputObj, outputObj);
	}

	private BaseArraySerializer createSerializerWithConfig(ExecutionConfig config) {
		return new BaseArraySerializer(
			DataTypes.RAW(TypeInformation.of(MyObj.class)).getLogicalType(), config);
	}

	@Override
	protected BaseArraySerializer createSerializer() {
		return new BaseArraySerializer(DataTypes.STRING().getLogicalType(), new ExecutionConfig());
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
		return new BaseArray[] {
				new GenericArray(new BinaryString[] {BinaryString.fromString("11")}, 1),
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
