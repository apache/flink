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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.BytewiseComparator;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BytewiseComparatorTest {

	@Test
	public void testBuiltComparator() {
		{
			BytewiseComparator<byte[]> bytearrayInstance = BytewiseComparator.BYTEARRAY_INSTANCE;
			byte[] b1 = new byte[]{(byte) 0, (byte) 1, (byte) 1};
			byte[] b2 = new byte[]{(byte) 0, (byte) 2};
			byte[] b3 = new byte[]{(byte) 3};
			assertTrue(bytearrayInstance.compare(b1, b2) < 0);
			assertTrue(bytearrayInstance.compare(b2, b2) == 0);
			assertTrue(bytearrayInstance.compare(b3, b2) > 0);
		}

		{
			BytewiseComparator<Byte> byteInstance = BytewiseComparator.BYTE_INSTANCE;
			assertTrue(byteInstance.compare((byte) 0, (byte) 2) < 0);
			assertTrue(byteInstance.compare((byte) 2, (byte) 2) == 0);
			assertTrue(byteInstance.compare((byte) 3, (byte) 2) > 0);
		}

		{
			BytewiseComparator<Integer> intInstance = BytewiseComparator.INT_INSTANCE;
			assertTrue(intInstance.compare(0, 2) < 0);
			assertTrue(intInstance.compare(2, 2) == 0);
			assertTrue(intInstance.compare(3, 2) > 0);
		}

		{
			BytewiseComparator<Long> longInstance = BytewiseComparator.LONG_INSTANCE;
			assertTrue(longInstance.compare(0L, 2L) < 0);
			assertTrue(longInstance.compare(2L, 2L) == 0);
			assertTrue(longInstance.compare(3L, 2L) > 0);
		}

		{
			BytewiseComparator<Float> floatInstance = BytewiseComparator.FLOAT_INSTANCE;
			assertTrue(floatInstance.compare(0.0f, 0.2f) < 0);
			assertTrue(floatInstance.compare(0.2f, 0.2f) == 0);
			assertTrue(floatInstance.compare(0.3f, 0.2f) > 0);
		}

		{
			BytewiseComparator<Double> doubleInstance = BytewiseComparator.DOUBLE_INSTANCE;
			assertTrue(doubleInstance.compare(0.0, 0.2) < 0);
			assertTrue(doubleInstance.compare(0.2, 0.2) == 0);
			assertTrue(doubleInstance.compare(0.3, 0.2) > 0);
		}
	}

}
