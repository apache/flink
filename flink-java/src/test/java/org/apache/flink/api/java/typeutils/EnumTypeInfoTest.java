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
import static org.junit.Assert.*;

public class EnumTypeInfoTest extends TestLogger {

	enum TestEnum {
		ONE, TWO
	}

	enum AlternativeEnum {
		ONE, TWO
	}

	@Test
	public void testEnumTypeEquality() {
		EnumTypeInfo<TestEnum> enumTypeInfo1 = new EnumTypeInfo<TestEnum>(TestEnum.class);
		EnumTypeInfo<TestEnum> enumTypeInfo2 = new EnumTypeInfo<TestEnum>(TestEnum.class);

		assertEquals(enumTypeInfo1, enumTypeInfo2);
		assertEquals(enumTypeInfo1.hashCode(), enumTypeInfo2.hashCode());
	}

	@Test
	public void testEnumTypeInequality() {
		EnumTypeInfo<TestEnum> enumTypeInfo1 = new EnumTypeInfo<TestEnum>(TestEnum.class);
		EnumTypeInfo<AlternativeEnum> enumTypeInfo2 = new EnumTypeInfo<AlternativeEnum>(AlternativeEnum.class);

		assertNotEquals(enumTypeInfo1, enumTypeInfo2);
	}
}
