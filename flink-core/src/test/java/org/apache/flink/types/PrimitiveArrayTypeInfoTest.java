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

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PrimitiveArrayTypeInfoTest extends TestLogger {

	static Class<?>[] classes = {int[].class, boolean[].class, byte[].class,
		short[].class, long[].class, float[].class, double[].class, char[].class};

	@Test
	public void testPrimitiveArrayTypeInfoEquality() {
		for (Class<?> clazz: classes) {
			PrimitiveArrayTypeInfo<?> tpeInfo1 = PrimitiveArrayTypeInfo.getInfoFor(clazz);
			PrimitiveArrayTypeInfo<?> tpeInfo2 = PrimitiveArrayTypeInfo.getInfoFor(clazz);

			assertEquals(tpeInfo1, tpeInfo2);
			assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
		}
	}

	@Test
	public void testBasicArrayTypeInfoInequality() {
		for (Class<?> clazz1: classes) {
			for (Class<?> clazz2: classes) {
				if (!clazz1.equals(clazz2)) {
					PrimitiveArrayTypeInfo<?> tpeInfo1 = PrimitiveArrayTypeInfo.getInfoFor(clazz1);
					PrimitiveArrayTypeInfo<?> tpeInfo2 = PrimitiveArrayTypeInfo.getInfoFor(clazz2);
					assertNotEquals(tpeInfo1, tpeInfo2);
				}
			}
		}
	}
}
