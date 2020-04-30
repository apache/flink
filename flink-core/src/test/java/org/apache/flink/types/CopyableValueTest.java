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

package org.apache.flink.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CopyableValueTest {

	@Test
	public void testCopy() {
		CopyableValue<?>[] value_types = new CopyableValue[] {
			new BooleanValue(true),
			new ByteValue((byte) 42),
			new CharValue('q'),
			new DoubleValue(3.1415926535897932),
			new FloatValue((float) 3.14159265),
			new IntValue(42),
			new LongValue(42L),
			new NullValue(),
			new ShortValue((short) 42),
			new StringValue("QED")
		};

		for (CopyableValue<?> type : value_types) {
			assertEquals(type, type.copy());
		}
	}

	@Test
	public void testCopyTo() {
		BooleanValue boolean_from = new BooleanValue(true);
		BooleanValue boolean_to = new BooleanValue(false);

		boolean_from.copyTo(boolean_to);
		assertEquals(boolean_from, boolean_to);

		ByteValue byte_from = new ByteValue((byte) 3);
		ByteValue byte_to = new ByteValue((byte) 7);

		byte_from.copyTo(byte_to);
		assertEquals(byte_from, byte_to);

		CharValue char_from = new CharValue('α');
		CharValue char_to = new CharValue('ω');

		char_from.copyTo(char_to);
		assertEquals(char_from, char_to);

		DoubleValue double_from = new DoubleValue(2.7182818284590451);
		DoubleValue double_to = new DoubleValue(0);

		double_from.copyTo(double_to);
		assertEquals(double_from, double_to);

		FloatValue float_from = new FloatValue((float) 2.71828182);
		FloatValue float_to = new FloatValue((float) 1.41421356);

		float_from.copyTo(float_to);
		assertEquals(float_from, float_to);

		IntValue int_from = new IntValue(8191);
		IntValue int_to = new IntValue(131071);

		int_from.copyTo(int_to);
		assertEquals(int_from, int_to);

		LongValue long_from = new LongValue(524287);
		LongValue long_to = new LongValue(2147483647);

		long_from.copyTo(long_to);
		assertEquals(long_from, long_to);

		NullValue null_from = new NullValue();
		NullValue null_to = new NullValue();

		null_from.copyTo(null_to);
		assertEquals(null_from, null_to);

		ShortValue short_from = new ShortValue((short) 31);
		ShortValue short_to = new ShortValue((short) 127);

		short_from.copyTo(short_to);
		assertEquals(short_from, short_to);

		StringValue string_from = new StringValue("2305843009213693951");
		StringValue string_to = new StringValue("618970019642690137449562111");

		string_from.copyTo(string_to);
		assertEquals(string_from, string_to);
	}
}
