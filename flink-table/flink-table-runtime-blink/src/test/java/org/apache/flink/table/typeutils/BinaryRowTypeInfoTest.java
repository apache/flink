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

package org.apache.flink.table.typeutils;

import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for {@link BaseRowTypeInfo}.
 */
public class BinaryRowTypeInfoTest {
	private static InternalType[] typeList = new InternalType[]{
			InternalTypes.INT,
			InternalTypes.STRING
	};

	@Test(expected = IllegalArgumentException.class)
	public void testWrongNumberOfFieldNames() {
		new BaseRowTypeInfo(typeList, new String[]{"int", "string", "int"});
		// number of field names should be equal to number of types, go fail
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDuplicateCustomFieldNames() {
		new BaseRowTypeInfo(typeList, new String[]{"int", "int"});
		// field names should not be the same, go fail
	}

	@Test
	public void testBinaryRowTypeInfoEquality() {
		BaseRowTypeInfo typeInfo1 = new BaseRowTypeInfo(
				InternalTypes.INT,
				InternalTypes.STRING);

		BaseRowTypeInfo typeInfo2 = new BaseRowTypeInfo(
				InternalTypes.INT,
				InternalTypes.STRING);

		assertEquals(typeInfo1, typeInfo2);
		assertEquals(typeInfo1.hashCode(), typeInfo2.hashCode());
	}

	@Test
	public void testBinaryRowTypeInfoInequality() {
		BaseRowTypeInfo typeInfo1 = new BaseRowTypeInfo(
				InternalTypes.INT,
				InternalTypes.STRING);

		BaseRowTypeInfo typeInfo2 = new BaseRowTypeInfo(
				InternalTypes.INT,
				InternalTypes.BOOLEAN);

		assertNotEquals(typeInfo1, typeInfo2);
		assertNotEquals(typeInfo1.hashCode(), typeInfo2.hashCode());
	}
}
