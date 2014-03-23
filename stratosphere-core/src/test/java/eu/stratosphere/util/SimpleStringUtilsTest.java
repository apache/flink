/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.SimpleStringUtils.WhitespaceTokenizer;


public class SimpleStringUtilsTest {

	@Test
	public void testToLowerCaseConverting() {
		StringValue testString = new StringValue("TEST");
		SimpleStringUtils.toLowerCase(testString);
		assertEquals(new StringValue("test"), testString);
	}
	
	@Test
	public void testReplaceNonWordChars() {
		StringValue testString = new StringValue("TEST123_@");
		SimpleStringUtils.replaceNonWordChars(testString, '!');
		assertEquals(new StringValue("TEST123_!"), testString);
	}
	
	@Test
	public void testTokenizerOnStringWithoutNexToken() {
		StringValue testString = new StringValue("test");
		SimpleStringUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		//first token
		tokenizer.next(testString);
		//next token is not exist
		assertFalse(tokenizer.next(testString));
	}
	
	@Test
	public void testTokenizerOnStringWithNexToken() {
		StringValue testString = new StringValue("test test");
		SimpleStringUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		assertTrue(tokenizer.next(testString));
	}
	
	@Test
	public void testTokenizerOnStringOnlyWithDelimiter() {
		StringValue testString = new StringValue("    ");
		SimpleStringUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
		tokenizer.setStringToTokenize(testString);
		assertFalse(tokenizer.next(testString));
	}
}
