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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import org.junit.Test;

import java.sql.Types;

import static org.apache.flink.connector.jdbc.utils.JdbcTypeUtil.typeInformationToSqlType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Testing the type conversions from Flink to SQL types.
 */
public class JdbcTypeUtilTest {

	@Test
	public void testTypeConversions() {
		assertEquals(Types.INTEGER, typeInformationToSqlType(BasicTypeInfo.INT_TYPE_INFO));
		testUnsupportedType(BasicTypeInfo.VOID_TYPE_INFO);
		testUnsupportedType(new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));
	}

	private static void testUnsupportedType(TypeInformation<?> type) {
		try {
			typeInformationToSqlType(type);
			fail();
		} catch (IllegalArgumentException ignored) {
		}
	}
}
