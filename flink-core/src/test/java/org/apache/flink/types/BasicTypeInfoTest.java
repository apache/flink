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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BasicTypeInfoTest extends TestLogger {

	static Class<?>[] classes = {String.class, Integer.class, Boolean.class, Byte.class,
		Short.class, Long.class, Float.class, Double.class, Character.class, Date.class,
		Void.class, BigInteger.class, BigDecimal.class};

	@Test
	public void testBasicTypeInfoEquality() {
		for (Class<?> clazz: classes) {
			BasicTypeInfo<?> tpeInfo1 = BasicTypeInfo.getInfoFor(clazz);
			BasicTypeInfo<?> tpeInfo2 = BasicTypeInfo.getInfoFor(clazz);

			assertEquals(tpeInfo1, tpeInfo2);
			assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
		}
	}

	@Test
	public void testBasicTypeInfoInequality() {
		for (Class<?> clazz1: classes) {
			for (Class<?> clazz2: classes) {
				if (!clazz1.equals(clazz2)) {
					BasicTypeInfo<?> tpeInfo1 = BasicTypeInfo.getInfoFor(clazz1);
					BasicTypeInfo<?> tpeInfo2 = BasicTypeInfo.getInfoFor(clazz2);
					assertNotEquals(tpeInfo1, tpeInfo2);
				}
			}
		}
	}
}
