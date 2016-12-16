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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RowTypeInfoTest {

	@Test
	public void testRowTypeInfoEquality() {
		RowTypeInfo typeInfo1 = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		RowTypeInfo typeInfo2 = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		assertEquals(typeInfo1, typeInfo2);
		assertEquals(typeInfo1.hashCode(), typeInfo2.hashCode());
	}

	@Test
	public void testRowTypeInfoInequality() {
		RowTypeInfo typeInfo1 = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		RowTypeInfo typeInfo2 = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.BOOLEAN_TYPE_INFO);

		assertNotEquals(typeInfo1, typeInfo2);
		assertNotEquals(typeInfo1.hashCode(), typeInfo2.hashCode());
	}

	@Test
	public void testNestedRowTypeInfo() {
		RowTypeInfo typeInfo = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			new RowTypeInfo(
				BasicTypeInfo.SHORT_TYPE_INFO,
			    BasicTypeInfo.BIG_DEC_TYPE_INFO
			),
			BasicTypeInfo.STRING_TYPE_INFO);

		assertEquals("Row(f0: Short, f1: BigDecimal)", typeInfo.getTypeAt("f1").toString());
		assertEquals("Short", typeInfo.getTypeAt("f1.f0").toString());
	}
}
