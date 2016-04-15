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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class ObjectArrayTypeInfoTest extends TestLogger {

	public static class TestClass{}

	@Test
	public void testObjectArrayTypeInfoEquality() {
		ObjectArrayTypeInfo<TestClass[], TestClass> tpeInfo1 = ObjectArrayTypeInfo.getInfoFor(
			TestClass[].class,
			new GenericTypeInfo<TestClass>(TestClass.class));

		ObjectArrayTypeInfo<TestClass[], TestClass> tpeInfo2 = ObjectArrayTypeInfo.getInfoFor(
			TestClass[].class,
			new GenericTypeInfo<TestClass>(TestClass.class));

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}

	@Test
	public void testObjectArrayTypeInfoInequality() {
		ObjectArrayTypeInfo<TestClass[], TestClass> tpeInfo1 = ObjectArrayTypeInfo.getInfoFor(
			TestClass[].class,
			new GenericTypeInfo<TestClass>(TestClass.class));

		ObjectArrayTypeInfo<TestClass[], TestClass> tpeInfo2 = ObjectArrayTypeInfo.getInfoFor(
			TestClass[].class,
			new PojoTypeInfo<TestClass>(TestClass.class, new ArrayList<PojoField>()));

		assertNotEquals(tpeInfo1, tpeInfo2);
	}
}
