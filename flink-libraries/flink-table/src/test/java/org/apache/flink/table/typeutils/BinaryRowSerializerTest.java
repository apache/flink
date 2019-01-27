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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.util.StringUtils;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link BinaryRowSerializer}.
 */
public class BinaryRowSerializerTest {

	@Test
	public void testRowSerializer() {
		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.STRING};
		BinaryRow row1 = new BinaryRow(2);
		BinaryRowWriter rowWriter1 = new BinaryRowWriter(row1);
		rowWriter1.writeInt(0, 1);
		rowWriter1.writeString(1, "a");
		rowWriter1.complete();

		BinaryRow row2 = new BinaryRow(2);
		BinaryRowWriter rowWriter2 = new BinaryRowWriter(row2);
		rowWriter2.writeInt(0, 2);
		rowWriter2.setNullAt(1);
		rowWriter2.complete();

		TypeSerializer<BinaryRow> serializer = new BinaryRowSerializer(types);
		BinaryRowSerializerTestInstance instance = new BinaryRowSerializerTestInstance(serializer, types, row1, row2);
		instance.testAll();
	}

	@Test
	public void testLargeRowSerializer() {
		TypeInformation[] types = new TypeInformation[]{
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
		};

		BinaryRow row = new BinaryRow(13);
		BinaryRowWriter rowWriter = new BinaryRowWriter(row);
		rowWriter.writeInt(0, 2);
		rowWriter.setNullAt(1);
		rowWriter.setNullAt(2);
		rowWriter.setNullAt(3);
		rowWriter.setNullAt(4);
		rowWriter.setNullAt(5);
		rowWriter.setNullAt(6);
		rowWriter.setNullAt(7);
		rowWriter.setNullAt(8);
		rowWriter.setNullAt(9);
		rowWriter.setNullAt(10);
		rowWriter.setNullAt(11);
		rowWriter.writeString(12, StringUtils.getRandomString(new Random(), 2000, 3000));
		rowWriter.complete();

		TypeSerializer<BinaryRow> serializer = new BinaryRowSerializer(types);
		BinaryRowSerializerTestInstance testInstance = new BinaryRowSerializerTestInstance(serializer, types, row);
		testInstance.testAll();
	}

	// ----------------------------------------------------------------------------------------------

	private class BinaryRowSerializerTestInstance extends SerializerTestInstance<BinaryRow> {

		private TypeInformation[] types;

		BinaryRowSerializerTestInstance(
				TypeSerializer<BinaryRow> serializer,
				TypeInformation[] types,
				BinaryRow... testData) {
			super(serializer, BinaryRow.class, -1, testData);
			this.types = types;
		}

		@Override
		protected void deepEquals(String message, BinaryRow should, BinaryRow is) {
			int arity = should.getArity();
			assertEquals(message, arity, is.getArity());
			for (int i = 0; i < arity; i++) {
				Object copiedValue = BaseRowUtil.get(should, i, types[i], null);
				Object element = BaseRowUtil.get(is, i, types[i], null);
				assertEquals(message, element, copiedValue);
			}
		}
	}

	@Test
	public void abstractRowSerializerCompatibilityTest() {
		BinaryRowSerializer rowSerializer = new BinaryRowSerializer(Types.INT, Types.STRING);
		TypeSerializerConfigSnapshot configSnapshot = rowSerializer.snapshotConfiguration();
		CompatibilityResult result = rowSerializer.ensureCompatibility(configSnapshot);
		assertEquals(result.isRequiresMigration(), false);
	}
}
