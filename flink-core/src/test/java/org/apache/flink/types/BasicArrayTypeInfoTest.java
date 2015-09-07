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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BasicArrayTypeInfoTest extends TestLogger {

	static Class<?>[] classes = {String[].class, Integer[].class, Boolean[].class, Byte[].class,
		Short[].class, Long[].class, Float[].class, Double[].class, Character[].class};

	@Test
	public void testBasicArrayTypeInfoEquality() {
		for (Class<?> clazz: classes) {
			BasicArrayTypeInfo<?, ?> tpeInfo1 = BasicArrayTypeInfo.getInfoFor(clazz);
			BasicArrayTypeInfo<?, ?> tpeInfo2 = BasicArrayTypeInfo.getInfoFor(clazz);

			assertEquals(tpeInfo1, tpeInfo2);
			assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
		}
	}

	@Test
	public void testBasicArrayTypeInfoInequality() {
		for (Class<?> clazz1: classes) {
			for (Class<?> clazz2: classes) {
				if (!clazz1.equals(clazz2)) {
					BasicArrayTypeInfo<?, ?> tpeInfo1 = BasicArrayTypeInfo.getInfoFor(clazz1);
					BasicArrayTypeInfo<?, ?> tpeInfo2 = BasicArrayTypeInfo.getInfoFor(clazz2);
					assertNotEquals(tpeInfo1, tpeInfo2);
				}
			}
		}
	}
}
