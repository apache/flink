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

import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class VarLengthStringParserTest {

    public StringValueParser parser = new StringValueParser();

    @Test
    void testGetValue() {
        Value v = parser.createValue();
        assertThat(v).isInstanceOf(StringValue.class);
    }

    @Test
    void testParseValidUnquotedStrings() {

        this.parser = new StringValueParser();

        // check valid strings without whitespaces and trailing delimiter
        byte[] recBytes = "abcdefgh|i|jklmno|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(9);
        assertThat(s.getValue()).isEqualTo("abcdefgh");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("i");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(18);
        assertThat(s.getValue()).isEqualTo("jklmno");

        // check single field not terminated
        recBytes = "abcde".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(5);
        assertThat(s.getValue()).isEqualTo("abcde");

        // check last field not terminated
        recBytes = "abcde|fg".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(6);
        assertThat(s.getValue()).isEqualTo("abcde");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(8);
        assertThat(s.getValue()).isEqualTo("fg");
    }

    @Test
    void testParseValidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings without whitespaces and trailing delimiter
        byte[] recBytes =
                "\"abcdefgh\"|\"i\"|\"jklmno\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcdefgh");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(15);
        assertThat(s.getValue()).isEqualTo("i");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(24);
        assertThat(s.getValue()).isEqualTo("jklmno");

        // check single field not terminated
        recBytes = "\"abcde\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(7);
        assertThat(s.getValue()).isEqualTo("abcde");

        // check last field not terminated
        recBytes = "\"abcde\"|\"fg\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(8);
        assertThat(s.getValue()).isEqualTo("abcde");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(12);
        assertThat(s.getValue()).isEqualTo("fg");

        // check delimiter in quotes
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcde|fg");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(26);
        assertThat(s.getValue()).isEqualTo("hij|kl|mn|op");

        // check delimiter in quotes last field not terminated
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcde|fg");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(25);
        assertThat(s.getValue()).isEqualTo("hij|kl|mn|op");
    }

    @Test
    void testParseValidMixedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '@');

        // check valid strings without whitespaces and trailing delimiter
        byte[] recBytes =
                "@abcde|gh@|@i@|jklmnopq|@rs@|tuv".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcde|gh");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(15);
        assertThat(s.getValue()).isEqualTo("i");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(24);
        assertThat(s.getValue()).isEqualTo("jklmnopq");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(29);
        assertThat(s.getValue()).isEqualTo("rs");

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(32);
        assertThat(s.getValue()).isEqualTo("tuv");
    }

    @Test
    void testParseInvalidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings without whitespaces and trailing delimiter
        byte[] recBytes = "\"abcdefgh\"-|\"jklmno  ".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isNegative();

        startPos = 12;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isNegative();
    }

    @Test
    void testParseValidMixedStringsWithCharset() {

        Charset charset = StandardCharsets.US_ASCII;
        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '@');

        // check valid strings without whitespaces and trailing delimiter
        byte[] recBytes =
                "@abcde|gh@|@i@|jklmnopq|@rs@|tuv".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        parser.setCharset(charset);
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcde|gh");
    }
}
