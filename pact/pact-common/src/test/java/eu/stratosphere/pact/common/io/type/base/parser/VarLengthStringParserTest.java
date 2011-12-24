package eu.stratosphere.pact.common.io.type.base.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;

public class VarLengthStringParserTest {

	public VarLengthStringParser parser = new VarLengthStringParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.getValue();
		assertTrue(v instanceof PactString);
	}
	
	@Test
	public void testConfigure() {
		
		// check invalid encapsulator
		boolean validConfig = true;
		Configuration config = new Configuration();
		config.setString(VarLengthStringParser.STRING_ENCAPSULATOR, "###");
		try {
			parser.configure(config);
		} catch (IllegalArgumentException iae) {
			validConfig = false;
		}
		assertFalse(validConfig);
		
		// check valid encapsulator
		validConfig = true;
		config = new Configuration();
		config.setString(VarLengthStringParser.STRING_ENCAPSULATOR, "\"");
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
		parser.configure(config);
		
		// check valid strings
		byte[] recBytes = "abcdefgh|i|jklmno|".getBytes();
		PactString s = new PactString();
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
		
		// check last field not terminated
		recBytes = "abcde".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 5);
		assertTrue(s.getValue().equals("abcde"));

		// check encapsulation
		config.setString(VarLengthStringParser.STRING_ENCAPSULATOR, "'");
		parser.configure(config);
		recBytes = "'abcdef'|'ghi'|'j kl  mn '|".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 9);
		assertTrue(s.getValue().equals("abcdef"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 15);
		assertTrue(s.getValue().equals("ghi"));
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 27);
		assertTrue(s.getValue().equals("j kl  mn "));
		
		// check corrupt encapsulation
		config.setString(VarLengthStringParser.STRING_ENCAPSULATOR, "'");
		parser.configure(config);
		recBytes = "abcdef'|'ghi".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == -1);
		startPos = 8;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == -1);
		
		// check last field not terminated
		recBytes = "'abcde'".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', s);
		assertTrue(startPos == 7);
		assertTrue(s.getValue().equals("abcde"));

	}
	
}
