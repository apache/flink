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

package org.apache.flink.types.parser;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.types.parser.FieldParser.ParseErrorState;

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

	@Test
	public void testNextStringEndPos() throws Exception {

		FieldParser parser = new TestFieldParser<String>();
		// single-char delimiter
		byte[] singleCharDelim = "|".getBytes(ConfigConstants.DEFAULT_CHARSET);

		byte[] bytes1 = "a|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		assertEquals(1, parser.nextStringEndPos(bytes1, 0, bytes1.length, singleCharDelim));
		assertEquals(-1, parser.nextStringEndPos(bytes1, 1, bytes1.length, singleCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(bytes1, 1, 1, singleCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(bytes1, 2, bytes1.length, singleCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		byte[] bytes2 = "a||".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(bytes2, 1, bytes2.length, singleCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		byte[] bytes3 = "a|c".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(bytes3, 1, bytes3.length, singleCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		parser.resetParserState();
		assertEquals(3, parser.nextStringEndPos(bytes3, 2, bytes3.length, singleCharDelim));
		assertEquals(ParseErrorState.NONE, parser.getErrorState());

		byte[] bytes4 = "a|c|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(3, parser.nextStringEndPos(bytes4, 2, bytes4.length, singleCharDelim));
		assertEquals(ParseErrorState.NONE, parser.getErrorState());

		// multi-char delimiter
		byte[] multiCharDelim = "|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		byte[] mBytes1 = "a|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(1, parser.nextStringEndPos(mBytes1, 0, mBytes1.length, multiCharDelim));
		assertEquals(-1, parser.nextStringEndPos(mBytes1, 1, mBytes1.length, multiCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(mBytes1, 1, 1, multiCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		byte[] mBytes2 = "a|#||#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(mBytes2, 1, mBytes2.length, multiCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		byte[] mBytes3 = "a|#|b".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(-1, parser.nextStringEndPos(mBytes3, 1, mBytes3.length, multiCharDelim));
		assertEquals(ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

		parser.resetParserState();
		assertEquals(5, parser.nextStringEndPos(mBytes3, 2, mBytes3.length, multiCharDelim));
		assertEquals(ParseErrorState.NONE, parser.getErrorState());

		byte[] mBytes4 = "a|#|b|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
		parser.resetParserState();
		assertEquals(5, parser.nextStringEndPos(mBytes4, 2, mBytes4.length, multiCharDelim));
		assertEquals(ParseErrorState.NONE, parser.getErrorState());

	}

}

/**
 * A FieldParser just for nextStringEndPos test.
 *
 * @param <T> The type that is parsed.
 */
class TestFieldParser<T> extends FieldParser<T>{

	@Override
	protected int parseField(byte[] bytes, int startPos, int limit, byte[] delim, T reuse) {
		return 0;
	}

	@Override
	public T getLastResult() {
		return null;
	}

	@Override
	public T createValue() {
		return null;
	}

	@Override
	protected void resetParserState() {
		super.resetParserState();
	}
}
