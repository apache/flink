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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RowTypeInfoTest {
	private static List<TypeInformation<?>> typeList = new ArrayList<>();


	@BeforeClass
	public static void setUp() throws Exception {
		typeList.add(BasicTypeInfo.INT_TYPE_INFO);
		typeList.add(new RowTypeInfo(
			BasicTypeInfo.SHORT_TYPE_INFO,
			BasicTypeInfo.BIG_DEC_TYPE_INFO));
		typeList.add(BasicTypeInfo.STRING_TYPE_INFO);
	}


	@Test
	public void testDuplicateCustomFieldNames() {
		// test number of field names should be equal to number of types
		try {
			new RowTypeInfo(typeList, Arrays.asList("int", "string"));
			fail("Expected an IllegalArgumentException, but no exception is thrown.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// test field names should not be the same.
		try {
			new RowTypeInfo(typeList, Arrays.asList("int", "string", "string"));
			fail("Expected an IllegalArgumentException, but no exception is thrown.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testCustomFieldNames() {
		RowTypeInfo typeInfo1 = new RowTypeInfo(typeList, Arrays.asList("int", "row", "string"));
		assertArrayEquals(new String[] {"int", "row", "string"}, typeInfo1.getFieldNames());

		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo1.getTypeAt("string"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo1.getTypeAt(2));
		assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, typeInfo1.getTypeAt("row.0"));
		assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, typeInfo1.getTypeAt("row.f1"));

		List<CompositeType.FlatFieldDescriptor> result = new ArrayList<>();
		typeInfo1.getFlatFields("row.*", 0, result);
		assertEquals(2, result.size());
	}

	@Test
	public void testGetTypeAt() {
		RowTypeInfo typeInfo = new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			new RowTypeInfo(
				BasicTypeInfo.SHORT_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO
			),
			BasicTypeInfo.STRING_TYPE_INFO);


		assertArrayEquals(new String[] {"f0", "f1", "f2"}, typeInfo.getFieldNames());

		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo.getTypeAt("f2"));
		assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, typeInfo.getTypeAt("f1.f0"));
		assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, typeInfo.getTypeAt("f1.1"));
	}

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
