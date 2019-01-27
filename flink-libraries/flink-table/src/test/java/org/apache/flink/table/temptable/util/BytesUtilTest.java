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

package org.apache.flink.table.temptable.util;

import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.table.temptable.util.BytesUtil.bytesToInt;
import static org.apache.flink.table.temptable.util.BytesUtil.intToBytes;

/**
 * Unit test for {@link BytesUtil}.
 */
public class BytesUtilTest {

	@Test
	public void testIntToBytes() {
		Assert.assertTrue(bytesToInt(intToBytes(1)) == 1);
		Assert.assertTrue(bytesToInt(intToBytes(-1)) == -1);
		Assert.assertTrue(bytesToInt(intToBytes(65536)) == 65536);
		Assert.assertTrue(bytesToInt(intToBytes(Integer.MIN_VALUE)) == Integer.MIN_VALUE);
		Assert.assertTrue(bytesToInt(intToBytes(Integer.MAX_VALUE)) == Integer.MAX_VALUE);
	}

	@Test
	public void testBinaryRowSerialize() {
		BinaryRow binaryRow = new BinaryRow(5);
		BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
		writer.writeInt(0, 100);
		writer.writeLong(1, 12345L);
		writer.writeString(2, "Hello World");
		writer.writeBoolean(3, false);
		writer.writeDouble(4, 1.123);
		writer.complete();

		BaseRowSerializer<BinaryRow> serializer =
			new BaseRowSerializer<>(
				DataTypes.INT,
				DataTypes.LONG,
				DataTypes.STRING,
				DataTypes.BOOLEAN,
				DataTypes.DOUBLE
			);

		byte[] bytes = BytesUtil.serialize(binaryRow, serializer);
		BaseRow result = BytesUtil.deSerialize(bytes, Integer.BYTES, bytes.length - Integer.BYTES, serializer);

		Assert.assertEquals(binaryRow.getArity(), result.getArity());
		Assert.assertEquals(binaryRow.getHeader(), result.getHeader());
		Assert.assertEquals(binaryRow.getInt(0), result.getInt(0));
		Assert.assertEquals(binaryRow.getLong(1), result.getLong(1));
		Assert.assertEquals(binaryRow.getString(2), result.getString(2));
		Assert.assertEquals(binaryRow.getBoolean(3), result.getBoolean(3));
		Assert.assertTrue(Math.abs(binaryRow.getDouble(4) - result.getDouble(4)) < 1e-6);
	}

}
