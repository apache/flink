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
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VarLengthStringParserTest {

    public StringValueParser parser = new StringValueParser();

    @Test
    public void testGetValue() {
        Value v = parser.createValue();
        assertTrue(v instanceof StringValue);
    }

    @Test
    public void testParseValidUnquotedStrings() {

        this.parser = new StringValueParser();

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes = "abcdefgh|i|jklmno|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(9, startPos);
        assertEquals("abcdefgh", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("i", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(18, startPos);
        assertEquals("jklmno", s.getValue());

        // check single field not terminated
        recBytes = "abcde".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(5, startPos);
        assertEquals("abcde", s.getValue());

        // check last field not terminated
        recBytes = "abcde|fg".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(6, startPos);
        assertEquals("abcde", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(8, startPos);
        assertEquals("fg", s.getValue());
    }

    @Test
    public void testParseValidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes =
                "\"abcdefgh\"|\"i\"|\"jklmno\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("abcdefgh", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(15, startPos);
        assertEquals("i", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(24, startPos);
        assertEquals("jklmno", s.getValue());

        // check single field not terminated
        recBytes = "\"abcde\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(7, startPos);
        assertEquals("abcde", s.getValue());

        // check last field not terminated
        recBytes = "\"abcde\"|\"fg\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(8, startPos);
        assertEquals("abcde", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(12, startPos);
        assertEquals("fg", s.getValue());

        // check delimiter in quotes
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("abcde|fg", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(26, startPos);
        assertEquals("hij|kl|mn|op", s.getValue());

        // check delimiter in quotes last field not terminated
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("abcde|fg", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(25, startPos);
        assertEquals("hij|kl|mn|op", s.getValue());
    }

    @Test
    public void testParseValidMixedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '@');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes =
                "@abcde|gh@|@i@|jklmnopq|@rs@|tuv".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("abcde|gh", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(15, startPos);
        assertEquals("i", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(24, startPos);
        assertEquals("jklmnopq", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(29, startPos);
        assertEquals("rs", s.getValue());

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(32, startPos);
        assertEquals("tuv", s.getValue());
    }

    @Test
    public void testParseInvalidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes = "\"abcdefgh\"-|\"jklmno  ".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertTrue(startPos < 0);

        startPos = 12;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertTrue(startPos < 0);
    }

    @Test
    public void testParseValidMixedStringsWithCharset() {

        Charset charset = StandardCharsets.US_ASCII;
        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '@');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes =
                "@abcde|gh@|@i@|jklmnopq|@rs@|tuv".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        parser.setCharset(charset);
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertEquals(11, startPos);
        assertEquals("abcde|gh", s.getValue());
    }
}
