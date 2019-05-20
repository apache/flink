/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.TimeZone;

import static org.apache.zookeeper.server.ServerCnxn.me;

/**
 * Test cases for class {@link SqlDateTimeUtils}.
 */
public class SqlDateTimeUtilsTest {

	@Test
	public void testParseToTimeMillis() throws Exception {
		String time1 = "1999-12-31 12:34:56.123";
		TimeZone tz = TimeZone.getDefault();

		// call private method by reflection.
		Class<?> clazz = SqlDateTimeUtils.class;
		Method method = clazz.getDeclaredMethod("parseToTimeMillis", String.class, TimeZone.class);
		method.setAccessible(true);
		Long result = (Long) method.invoke(null, time1, tz);

		Assert.assertEquals(123L, result % 1000);

		String time2 = "1999-12-31 12:34:56.123456";
		result = (Long) method.invoke(null, time2, tz);

		Assert.assertEquals(123, result % 1000);
	}
}
