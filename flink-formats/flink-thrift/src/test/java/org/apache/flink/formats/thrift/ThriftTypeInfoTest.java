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

package org.apache.flink.formats.thrift;

import org.apache.flink.formats.thrift.generated.Event;
import org.apache.flink.formats.thrift.typeutils.ThriftTypeInfo;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Tests for ThriftTypeInfo class.
 */
public class ThriftTypeInfoTest {

	@Test
	public void testThriftTypeInfo() {
		ThriftTypeInfo thriftTypeInfo = new ThriftTypeInfo(Event.class, ThriftCodeGenerator.THRIFT);
		String[] fieldNames = thriftTypeInfo.getFieldNames();
		for (String str : fieldNames) {
			System.out.println(str);
		}
		assertEquals(fieldNames.length, 6);
	}
}
