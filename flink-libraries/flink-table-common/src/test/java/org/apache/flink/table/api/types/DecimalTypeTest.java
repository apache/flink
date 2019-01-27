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

package org.apache.flink.table.api.types;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit Test for DecimalType class.
 */
public class DecimalTypeTest {

	@Test
	public void testParseString() {
		String s = "decimal(5,2)";
		DecimalType dt = DecimalType.of(s);
		assertTrue(dt.precision() == 5);
		assertTrue(dt.scale() == 2);
	}

	@Test
	public void testParseString1() {
		String s = "decim(15,3)";
		try {
			DecimalType dt = DecimalType.of(s);
		} catch (IllegalArgumentException ex) {
			return;
		}
		fail(s + " should not be a valid decimal type name.");
	}

	@Test
	public void testParseString2() {
		String s = "decimal(15,s)";
		try {
			DecimalType dt = DecimalType.of(s);
		} catch (IllegalArgumentException ex) {
			return;
		}
		fail(s + " should not be a valid decimal type name.");
	}

}
