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

package org.apache.flink.util;

import org.apache.flink.types.StringValue;
import org.apache.flink.util.StringValueUtils.WhitespaceTokenizer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StringValueUtils}.
 */
public class StringValueUtilsTest extends TestLogger {

	@Test
	public void testToLowerCaseConverting() {
		StringValue testString = new StringValue("TEST");
		StringValueUtils.toLowerCase(testString);
		assertEquals(new StringValue("test"), testString);
	}

	@Test
	public void testReplaceNonWordChars() {
		StringValue testString = new StringValue("TEST123_@");
		StringValueUtils.replaceNonWordChars(testString, '!');
		assertEquals(new StringValue("TEST123_!"), testString);
	}

	@Test
	public void testTokenizerOnStringWithoutNexToken() {
		StringValue testString = new StringValue("test");
		StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		//first token
		tokenizer.next(testString);
		//next token is not exist
		assertFalse(tokenizer.next(testString));
	}

	@Test
	public void testTokenizerOnStringWithNexToken() {
		StringValue testString = new StringValue("test test");
		StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		assertTrue(tokenizer.next(testString));
	}

	@Test
	public void testTokenizerOnStringOnlyWithDelimiter() {
		StringValue testString = new StringValue("    ");
		StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		assertFalse(tokenizer.next(testString));
	}
}
