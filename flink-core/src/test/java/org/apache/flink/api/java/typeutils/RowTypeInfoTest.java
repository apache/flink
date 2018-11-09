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
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link RowTypeInfo}.
 */
public class RowTypeInfoTest extends TypeInformationTestBase<RowTypeInfo> {
	private static TypeInformation<?>[] typeList = new TypeInformation<?>[]{
		BasicTypeInfo.INT_TYPE_INFO,
		new RowTypeInfo(
			BasicTypeInfo.SHORT_TYPE_INFO,
			BasicTypeInfo.BIG_DEC_TYPE_INFO),
		BasicTypeInfo.STRING_TYPE_INFO
	};

	@Override
	protected RowTypeInfo[] getTestData() {
		return new RowTypeInfo[] {
			new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
			new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO),
			new RowTypeInfo(typeList),
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"int", "int2"})
		};
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongNumberOfFieldNames() {
		new RowTypeInfo(typeList, new String[]{"int", "string"});
		// number of field names should be equal to number of types, go fail
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDuplicateCustomFieldNames() {
		new RowTypeInfo(typeList, new String[]{"int", "string", "string"});
		// field names should not be the same, go fail
	}

	@Test
	public void testCustomFieldNames() {
		String[] fieldNames = new String[]{"int", "row", "string"};
		RowTypeInfo typeInfo1 = new RowTypeInfo(typeList, new String[]{"int", "row", "string"});
		assertArrayEquals(new String[]{"int", "row", "string"}, typeInfo1.getFieldNames());

		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo1.getTypeAt("string"));
		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo1.getTypeAt(2));
		assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, typeInfo1.getTypeAt("row.0"));
		assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, typeInfo1.getTypeAt("row.f1"));

		// change the names in fieldNames
		fieldNames[1] = "composite";
		RowTypeInfo typeInfo2 = new RowTypeInfo(typeList, fieldNames);
		// make sure the field names are deep copied
		assertArrayEquals(new String[]{"int", "row", "string"}, typeInfo1.getFieldNames());
		assertArrayEquals(new String[]{"int", "composite", "string"}, typeInfo2.getFieldNames());
	}

	@Test
	public void testGetFlatFields() {
		RowTypeInfo typeInfo1 = new RowTypeInfo(typeList, new String[]{"int", "row", "string"});
		List<FlatFieldDescriptor> result = new ArrayList<>();
		typeInfo1.getFlatFields("row.*", 0, result);
		assertEquals(2, result.size());
		assertEquals(
			new FlatFieldDescriptor(1, BasicTypeInfo.SHORT_TYPE_INFO).toString(),
			result.get(0).toString());
		assertEquals(
			new FlatFieldDescriptor(2, BasicTypeInfo.BIG_DEC_TYPE_INFO).toString(),
			result.get(1).toString());

		result.clear();
		typeInfo1.getFlatFields("string", 0, result);
		assertEquals(1, result.size());
		assertEquals(
			new FlatFieldDescriptor(3, BasicTypeInfo.STRING_TYPE_INFO).toString(),
			result.get(0).toString());
	}

	@Test
	public void testGetTypeAt() {
		RowTypeInfo typeInfo = new RowTypeInfo(typeList);


		assertArrayEquals(new String[]{"f0", "f1", "f2"}, typeInfo.getFieldNames());

		assertEquals(BasicTypeInfo.STRING_TYPE_INFO, typeInfo.getTypeAt("f2"));
		assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, typeInfo.getTypeAt("f1.f0"));
		assertEquals(BasicTypeInfo.BIG_DEC_TYPE_INFO, typeInfo.getTypeAt("f1.1"));
	}

	@Test
	public void testNestedRowTypeInfo() {
		RowTypeInfo typeInfo = new RowTypeInfo(typeList);

		assertEquals("Row(f0: Short, f1: BigDecimal)", typeInfo.getTypeAt("f1").toString());
		assertEquals("Short", typeInfo.getTypeAt("f1.f0").toString());
	}

	@Test
	public void testSchemaEquals() {
		final RowTypeInfo row1 = new RowTypeInfo(
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"field1", "field2"});
		final RowTypeInfo row2 = new RowTypeInfo(
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"field1", "field2"});
		assertTrue(row1.schemaEquals(row2));

		final RowTypeInfo other1 = new RowTypeInfo(
			new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"otherField", "field2"});
		final RowTypeInfo other2 = new RowTypeInfo(
			new TypeInformation[]{BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"field1", "field2"});
		assertFalse(row1.schemaEquals(other1));
		assertFalse(row1.schemaEquals(other2));
	}

}
