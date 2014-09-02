/**
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

import static org.junit.Assert.assertTrue;

import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.types.parser.StringValueParser;
import org.junit.Test;

public class VarLengthStringParserTest {

	public StringValueParser parser = new StringValueParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.createValue();
		assertTrue(v instanceof StringValue);
	}
	
	@Test
	public void testParseValidUnquotedStrings() {
		
		// check valid strings with out whitespaces and trailing delimiter
		byte[] recBytes = "abcdefgh|i|jklmno|".getBytes();
		StringValue s = new StringValue();
		
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 9);
		assertTrue(s.getValue().equals("abcdefgh"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 11);
		assertTrue(s.getValue().equals("i"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 18);
		assertTrue(s.getValue().equals("jklmno"));
		
		
		// check single field not terminated
		recBytes = "abcde".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 5);
		assertTrue(s.getValue().equals("abcde"));
		
		// check last field not terminated
		recBytes = "abcde|fg".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 6);
		assertTrue(s.getValue().equals("abcde"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 8);
		assertTrue(s.getValue().equals("fg"));
	}
	
	@Test
	public void testParseValidQuotedStringsWithoutWhitespaces() {
		
		// check valid strings with out whitespaces and trailing delimiter
		byte[] recBytes = "\"abcdefgh\"|\"i\"|\"jklmno\"|".getBytes();
		StringValue s = new StringValue();
		
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 11);
		assertTrue(s.getValue().equals("abcdefgh"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 15);
		assertTrue(s.getValue().equals("i"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 24);
		assertTrue(s.getValue().equals("jklmno"));
		
		
		// check single field not terminated
		recBytes = "\"abcde\"".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 7);
		assertTrue(s.getValue().equals("abcde"));
		
		// check last field not terminated
		recBytes = "\"abcde\"|\"fg\"".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 8);
		assertTrue(s.getValue().equals("abcde"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 12);
		assertTrue(s.getValue().equals("fg"));
		
		// check delimiter in quotes 
		recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 11);
		assertTrue(s.getValue().equals("abcde|fg"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 26);
		assertTrue(s.getValue().equals("hij|kl|mn|op"));
		
		// check delimiter in quotes last field not terminated
		recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 11);
		assertTrue(s.getValue().equals("abcde|fg"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 25);
		assertTrue(s.getValue().equals("hij|kl|mn|op"));
	}
	
	@Test
	public void testParseValidQuotedStringsWithWhitespaces() {
		
		// check valid strings with out whitespaces and trailing delimiter
		byte[] recBytes = "  \"abcdefgh\"|     \"i\"\t\t\t|\t \t\"jklmno\"  |".getBytes();
		StringValue s = new StringValue();
		
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 13);
		assertTrue(s.getValue().equals("abcdefgh"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 25);
		assertTrue(s.getValue().equals("i"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 39);
		assertTrue(s.getValue().equals("jklmno"));
		
		// check valid strings with out whitespaces without trailing delimiter
		recBytes = "  \"abcdefgh\"|     \"i\"\t\t\t|\t \t\"jklmno\"  ".getBytes();
		s = new StringValue();
		
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 13);
		assertTrue(s.getValue().equals("abcdefgh"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 25);
		assertTrue(s.getValue().equals("i"));
		
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 38);
		assertTrue(s.getValue().equals("jklmno"));
		
		// check single field not terminated
		recBytes = "  \t\"abcde\"\t\t  \t ".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 16);
		assertTrue(s.getValue().equals("abcde"));
		
		// check single field terminated
		recBytes = "  \t\"abcde\"\t\t  \t |".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 17);
		assertTrue(s.getValue().equals("abcde"));
	}
	
	@Test
	public void testParseInvalidQuotedStrings() {
		
		// check valid strings with out whitespaces and trailing delimiter
		byte[] recBytes = "  \"abcdefgh\" gh |     \"i\"\t\t\t|\t \t\"jklmno\"  |".getBytes();
		StringValue s = new StringValue();
		
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos < 0);
	}
}
