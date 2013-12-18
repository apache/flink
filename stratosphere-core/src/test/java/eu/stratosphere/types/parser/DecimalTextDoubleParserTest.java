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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.Value;
import eu.stratosphere.types.parser.DecimalTextDoubleParser;

public class DecimalTextDoubleParserTest {

	public DecimalTextDoubleParser parser = new DecimalTextDoubleParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.createValue();
		assertTrue(v instanceof PactDouble);
	}
	
	@Test
	public void testParseFieldWithScientificNotation() {
		
		byte[] recBytes = "123.4|0.124|.623|1234|-12.34|123abc4|".getBytes();
		
		// check simple valid double
		PactDouble d = new PactDouble();
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 6);
		assertTrue(d.getValue() == 123.4);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 12);
		assertTrue(d.getValue() == 0.124);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 17);
		assertTrue(d.getValue() == 0.623);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 22);
		assertTrue(d.getValue() == 1234);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 29);
		assertTrue(d.getValue() == -12.34);
		
		// check invalid chars
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos < 0);
		
		// check last field not terminated
		recBytes = "12.34".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 5);
		assertTrue(d.getValue() == 12.34);
		
		// check scientific notation
		recBytes = "1.234E2|1.234e3|1.234E-2".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 8);
		assertTrue(d.getValue() == 123.4);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 16);
		assertTrue(d.getValue() == 1234.0);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == recBytes.length);
		assertTrue(d.getValue() == 0.01234);
		
	}
}
