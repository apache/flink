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