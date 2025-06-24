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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FieldParserTest {

    @Test
    void testDelimiterNext() {
        byte[] bytes = "aaabc".getBytes();
        byte[] delim = "aa".getBytes();
        assertThat(FieldParser.delimiterNext(bytes, 0, delim)).isTrue();
        assertThat(FieldParser.delimiterNext(bytes, 1, delim)).isTrue();
        assertThat(FieldParser.delimiterNext(bytes, 2, delim)).isFalse();
    }

    @Test
    void testEndsWithDelimiter() {
        byte[] bytes = "aabc".getBytes();
        byte[] delim = "ab".getBytes();
        assertThat(FieldParser.endsWithDelimiter(bytes, 0, delim)).isFalse();
        assertThat(FieldParser.endsWithDelimiter(bytes, 1, delim)).isFalse();
        assertThat(FieldParser.endsWithDelimiter(bytes, 2, delim)).isTrue();
        assertThat(FieldParser.endsWithDelimiter(bytes, 3, delim)).isFalse();
    }

    @Test
    void testNextStringEndPos() {

        FieldParser parser = new TestFieldParser<String>();
        // single-char delimiter
        byte[] singleCharDelim = "|".getBytes(ConfigConstants.DEFAULT_CHARSET);

        byte[] bytes1 = "a|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        assertThat(parser.nextStringEndPos(bytes1, 0, bytes1.length, singleCharDelim)).isOne();
        assertThat(parser.nextStringEndPos(bytes1, 1, bytes1.length, singleCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes1, 1, 1, singleCharDelim)).isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes1, 2, bytes1.length, singleCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        byte[] bytes2 = "a||".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes2, 1, bytes2.length, singleCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        byte[] bytes3 = "a|c".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes3, 1, bytes3.length, singleCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes3, 2, bytes3.length, singleCharDelim)).isEqualTo(3);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.NONE);

        byte[] bytes4 = "a|c|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(bytes4, 2, bytes4.length, singleCharDelim)).isEqualTo(3);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.NONE);

        // multi-char delimiter
        byte[] multiCharDelim = "|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        byte[] mBytes1 = "a|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes1, 0, mBytes1.length, multiCharDelim)).isOne();
        assertThat(parser.nextStringEndPos(mBytes1, 1, mBytes1.length, multiCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes1, 1, 1, multiCharDelim)).isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        byte[] mBytes2 = "a|#||#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes2, 1, mBytes2.length, multiCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        byte[] mBytes3 = "a|#|b".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes3, 1, mBytes3.length, multiCharDelim))
                .isEqualTo(-1);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.EMPTY_COLUMN);

        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes3, 2, mBytes3.length, multiCharDelim))
                .isEqualTo(5);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.NONE);

        byte[] mBytes4 = "a|#|b|#|".getBytes(ConfigConstants.DEFAULT_CHARSET);
        parser.resetParserState();
        assertThat(parser.nextStringEndPos(mBytes4, 2, mBytes4.length, multiCharDelim))
                .isEqualTo(5);
        assertThat(parser.getErrorState()).isEqualTo(ParseErrorState.NONE);
    }
}

/**
 * A FieldParser just for nextStringEndPos test.
 *
 * @param <T> The type that is parsed.
 */
class TestFieldParser<T> extends FieldParser<T> {

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
