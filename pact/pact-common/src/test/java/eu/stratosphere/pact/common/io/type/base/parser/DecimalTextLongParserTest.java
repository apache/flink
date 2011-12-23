package eu.stratosphere.pact.common.io.type.base.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;

public class DecimalTextLongParserTest {

	public DecimalTextLongParser parser = new DecimalTextLongParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.getValue();
		assertTrue(v instanceof PactLong);
	}
	
	@Test
	public void testParseField() {
		
byte[] recBytes = "1234567890123456789|-1234567890123456789|123abc4|".getBytes();
		
		// check valid int
		PactLong l = new PactLong();
		int startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 20);
		assertTrue(l.getValue() == 1234567890123456789l);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 41);
		assertTrue(l.getValue() == -1234567890123456789l);
		
		// check invalid chars
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos < 0);
		
		// check last field not terminated
		recBytes = "1234".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 4);
		assertTrue(l.getValue() == 1234);

		// check parsing multiple fields
		recBytes = "12|34|56|78|90".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 3);
		assertTrue(l.getValue() == 12);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 6);
		assertTrue(l.getValue() == 34);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 9);
		assertTrue(l.getValue() == 56);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 12);
		assertTrue(l.getValue() == 78);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', l);
		assertTrue(startPos == 14);
		assertTrue(l.getValue() == 90);
		
	}
	
}
