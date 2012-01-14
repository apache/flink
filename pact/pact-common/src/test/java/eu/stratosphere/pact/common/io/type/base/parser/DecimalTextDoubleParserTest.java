package eu.stratosphere.pact.common.io.type.base.parser;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;

public class DecimalTextDoubleParserTest {

	public DecimalTextDoubleParser parser = new DecimalTextDoubleParser();
	
	@Test
	public void testGetValue() {
		Value v = parser.getValue();
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

		// check decimal separator
		Configuration config = new Configuration();
		config.setString(DecimalTextDoubleParser.DECIMAL_SEPARATOR, ",");
		parser.configure(config);
		
		recBytes = "124,56|12.34".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 7);
		assertTrue(d.getValue() == 124.56);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == -1);
		parser.configure(new Configuration());
		
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
	
	@Test
	public void testParseFieldWithoutScientificNotation() {
		
		Configuration config = new Configuration();
		config.setBoolean(DecimalTextDoubleParser.SCIENTIFIC_NOTATION_ENABLED, false);
		
		// check simple valid double
		byte[] recBytes = "123.4|0.124|.623|1234|-12.34|123abc4|".getBytes();
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

		// check decimal separator
		config.setString(DecimalTextDoubleParser.DECIMAL_SEPARATOR, ",");
		parser.configure(config);
		
		recBytes = "124,56|12.34".getBytes();
		startPos = 0;
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == 7);
		assertTrue(d.getValue() == 124.56);
		startPos = parser.parseField(recBytes, startPos, recBytes.length, '|', d);
		assertTrue(startPos == -1);
		
	}
	
}
