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

package org.apache.flink.types.parser;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldParserTest {

	@Test
	public void testDelimiterNext() throws Exception {
		byte[] bytes = "aaabc".getBytes();
		byte[] delim = "aa".getBytes();
		assertTrue(FieldParser.delimiterNext(bytes, 0, delim));
		assertTrue(FieldParser.delimiterNext(bytes, 1, delim));
		assertFalse(FieldParser.delimiterNext(bytes, 2, delim));
	}

	@Test
	public void testEndsWithDelimiter() throws Exception {
		byte[] bytes = "aabc".getBytes();
		byte[] delim = "ab".getBytes();
		assertFalse(FieldParser.endsWithDelimiter(bytes, 0, delim));
		assertFalse(FieldParser.endsWithDelimiter(bytes, 1, delim));
		assertTrue(FieldParser.endsWithDelimiter(bytes, 2, delim));
		assertFalse(FieldParser.endsWithDelimiter(bytes, 3, delim));
	}

}