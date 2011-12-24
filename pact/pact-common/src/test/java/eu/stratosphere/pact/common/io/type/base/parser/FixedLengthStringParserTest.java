package eu.stratosphere.pact.common.io.type.base.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.FixedLengthStringParser;

public class FixedLengthStringParserTest {

	public FixedLengthStringParser parser = new FixedLengthStringParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.getValue();
		assertTrue(v instanceof PactString);
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
		PactString s = new PactString();
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
		recBytes = "'abcde'|'efghi'|'jklmn|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 8);
		assertTrue(s.getValue().equals("abcde"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 16);
		assertTrue(s.getValue().equals("efghi"));
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
