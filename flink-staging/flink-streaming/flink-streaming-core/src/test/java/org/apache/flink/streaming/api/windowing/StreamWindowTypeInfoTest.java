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

package org.apache.flink.streaming.api.windowing;

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class StreamWindowTypeInfoTest extends TestLogger {

	public static class TestClass{}

	@Test
	public void testStreamWindowTypeInfoEquality() {
		StreamWindowTypeInfo<TestClass> tpeInfo1 = new StreamWindowTypeInfo<>(new GenericTypeInfo<>(TestClass.class));
		StreamWindowTypeInfo<TestClass> tpeInfo2 = new StreamWindowTypeInfo<>(new GenericTypeInfo<>(TestClass.class));

		assertEquals(tpeInfo1, tpeInfo2);
		assertEquals(tpeInfo1.hashCode(), tpeInfo2.hashCode());
	}

	@Test
	public void testStreamWindowTypeInfoInequality() {
		StreamWindowTypeInfo<TestClass> tpeInfo1 = new StreamWindowTypeInfo<>(new GenericTypeInfo<>(TestClass.class));
		StreamWindowTypeInfo<TestClass> tpeInfo2 = new StreamWindowTypeInfo<>(new PojoTypeInfo<>(TestClass.class, new ArrayList<PojoField>()));

		assertNotEquals(tpeInfo1, tpeInfo2);
	}
}
