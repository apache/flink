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

public class VarLengthStringParserTest {

    public StringValueParser parser = new StringValueParser();

    @Test
    void testGetValue() {
        Value v = parser.createValue();
        assertThat(v).isInstanceOf(StringValue.class);
    }

    @Test
    void testParseValidUnquotedStrings() {

        this.parser = new StringValueParser();

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes = "abcdefgh|i|jklmno|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 9).isTrue();
        assertThat(s.getValue().equals("abcdefgh")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 11).isTrue();
        assertThat(s.getValue().equals("i")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 18).isTrue();
        assertThat(s.getValue().equals("jklmno")).isTrue();

        // check single field not terminated
        recBytes = "abcde".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 5).isTrue();
        assertThat(s.getValue().equals("abcde")).isTrue();

        // check last field not terminated
        recBytes = "abcde|fg".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 6).isTrue();
        assertThat(s.getValue().equals("abcde")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 8).isTrue();
        assertThat(s.getValue().equals("fg")).isTrue();
    }

    @Test
    void testParseValidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes =
                "\"abcdefgh\"|\"i\"|\"jklmno\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 11).isTrue();
        assertThat(s.getValue().equals("abcdefgh")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 15).isTrue();
        assertThat(s.getValue().equals("i")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 24).isTrue();
        assertThat(s.getValue().equals("jklmno")).isTrue();

        // check single field not terminated
        recBytes = "\"abcde\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 7).isTrue();
        assertThat(s.getValue().equals("abcde")).isTrue();

        // check last field not terminated
        recBytes = "\"abcde\"|\"fg\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 8).isTrue();
        assertThat(s.getValue().equals("abcde")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 12).isTrue();
        assertThat(s.getValue().equals("fg")).isTrue();

        // check delimiter in quotes
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 11).isTrue();
        assertThat(s.getValue().equals("abcde|fg")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 26).isTrue();
        assertThat(s.getValue().equals("hij|kl|mn|op")).isTrue();

        // check delimiter in quotes last field not terminated
        recBytes = "\"abcde|fg\"|\"hij|kl|mn|op\"".getBytes(ConfigConstants.DEFAULT_CHARSET);
        startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 11).isTrue();
        assertThat(s.getValue().equals("abcde|fg")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 25).isTrue();
        assertThat(s.getValue().equals("hij|kl|mn|op")).isTrue();
    }

    @Test
    void testParseValidMixedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '@');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes =
                "@abcde|gh@|@i@|jklmnopq|@rs@|tuv".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 11).isTrue();
        assertThat(s.getValue().equals("abcde|gh")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 15).isTrue();
        assertThat(s.getValue().equals("i")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 24).isTrue();
        assertThat(s.getValue().equals("jklmnopq")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 29).isTrue();
        assertThat(s.getValue().equals("rs")).isTrue();

        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos == 32).isTrue();
        assertThat(s.getValue().equals("tuv")).isTrue();
    }

    @Test
    void testParseInvalidQuotedStrings() {

        this.parser = new StringValueParser();
        this.parser.enableQuotedStringParsing((byte) '"');

        // check valid strings with out whitespaces and trailing delimiter
        byte[] recBytes = "\"abcdefgh\"-|\"jklmno  ".getBytes(ConfigConstants.DEFAULT_CHARSET);
        StringValue s = new StringValue();

        int startPos = 0;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos < 0).isTrue();

        startPos = 12;
        startPos = parser.parseField(recBytes, startPos, recBytes.length, new byte[] {'|'}, s);
        assertThat(startPos < 0).isTrue();
    }

    @Test
    void testParseValidMixedStringsWithCharset() {

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
        assertThat(startPos).isEqualTo(11);
        assertThat(s.getValue()).isEqualTo("abcde|gh");
    }
}
