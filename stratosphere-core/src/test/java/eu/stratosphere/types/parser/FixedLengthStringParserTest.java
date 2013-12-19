/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.types.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.types.parser.FixedLengthStringParser;

public class FixedLengthStringParserTest {

	public FixedLengthStringParser parser = new FixedLengthStringParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.createValue();
		assertTrue(v instanceof StringValue);
	}
	
	@Test
	public void testConfigure() {
		
		// check missing length
		boolean validConfig = true;
		Configuration config = new Configuration();
		try {
			parser.configure(config);
		} catch (IllegalArgumentException iae) {
			validConfig = false;
		}
		assertFalse(validConfig);
		
		// check with length
		validConfig = true;
		config = new Configuration();
		config.setInteger(FixedLengthStringParser.STRING_LENGTH, 2);
		try {
			parser.configure(config);
		} catch (IllegalArgumentException iae) {
			validConfig = false;
		}
		assertTrue(validConfig);
		
		// check invalid encapsulator
		validConfig = true;
		config = new Configuration();
		config.setInteger(FixedLengthStringParser.STRING_LENGTH, 2);
		config.setString(FixedLengthStringParser.STRING_ENCAPSULATOR, "###");
		try {
			parser.configure(config);
		} catch (IllegalArgumentException iae) {
			validConfig = false;
		}
		assertFalse(validConfig);
		
		// check valid encapsulator
		validConfig = true;
		config = new Configuration();
		config.setInteger(FixedLengthStringParser.STRING_LENGTH, 2);
		config.setString(FixedLengthStringParser.STRING_ENCAPSULATOR, "\"");
		try {
			parser.configure(config);
		} catch (IllegalArgumentException iae) {
			validConfig = false;
		}
		assertTrue(validConfig);
	}
	
	@Test
	public void testParseField() {
		
		Configuration config = new Configuration();
		config.setInteger(FixedLengthStringParser.STRING_LENGTH, 5);
		parser.configure(config);
		
		// check valid strings
		byte[] recBytes = "abcde|efghi|jklmn|".getBytes();
		StringValue s = new StringValue();
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 6);
		assertTrue(s.getValue().equals("abcde"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 12);
		assertTrue(s.getValue().equals("efghi"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 18);
		assertTrue(s.getValue().equals("jklmn"));
		
		// check last field not terminated
		recBytes = "abcde".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 5);
		assertTrue(s.getValue().equals("abcde"));

		// check longer field
		recBytes = "abcdef|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == -1);
		
		// check encapsulation
		config.setString(FixedLengthStringParser.STRING_ENCAPSULATOR, "'");
		config.setInteger(FixedLengthStringParser.STRING_LENGTH, 7);
		parser.configure(config);
		recBytes = "'abcde'|'e  f '|'jklmn|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 8);
		assertTrue(s.getValue().equals("abcde"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 16);
		assertTrue(s.getValue().equals("e  f "));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == -1);
		
		// check last field not terminated
		recBytes = "'abcde'".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 7);
		assertTrue(s.getValue().equals("abcde"));

		// check longer field
		recBytes = "'abcdef'|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == -1);
		
	}
	
	
}
