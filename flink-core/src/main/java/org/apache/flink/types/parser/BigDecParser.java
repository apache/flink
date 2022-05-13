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

import org.apache.flink.annotation.PublicEvolving;

import java.math.BigDecimal;

/** Parses a text field into a {@link java.math.BigDecimal}. */
@PublicEvolving
public class BigDecParser extends FieldParser<BigDecimal> {

    private static final BigDecimal BIG_DECIMAL_INSTANCE = BigDecimal.ZERO;

    private BigDecimal result;
    private char[] reuse = null;

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, BigDecimal reusable) {
        final int endPos = nextStringEndPos(bytes, startPos, limit, delimiter);
        if (endPos < 0) {
            return -1;
        }

        try {
            final int length = endPos - startPos;
            if (reuse == null || reuse.length < length) {
                reuse = new char[length];
            }
            for (int j = 0; j < length; j++) {
                final byte b = bytes[startPos + j];
                if ((b < '0' || b > '9')
                        && b != '-'
                        && b != '+'
                        && b != '.'
                        && b != 'E'
                        && b != 'e') {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
                    return -1;
                }
                reuse[j] = (char) bytes[startPos + j];
            }

            this.result = new BigDecimal(reuse, 0, length);
            return (endPos == limit) ? limit : endPos + delimiter.length;
        } catch (NumberFormatException e) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
    }

    @Override
    public BigDecimal createValue() {
        return BIG_DECIMAL_INSTANCE;
    }

    @Override
    public BigDecimal getLastResult() {
        return this.result;
    }

    /**
     * Static utility to parse a field of type BigDecimal from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final BigDecimal parseField(byte[] bytes, int startPos, int length) {
        return parseField(bytes, startPos, length, (char) 0xffff);
    }

    /**
     * Static utility to parse a field of type BigDecimal from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @param delimiter The delimiter that terminates the field.
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final BigDecimal parseField(
            byte[] bytes, int startPos, int length, char delimiter) {
        if (length <= 0) {
            throw new NumberFormatException("Invalid input: Empty string");
        }
        int i = 0;
        final byte delByte = (byte) delimiter;

        while (i < length && bytes[startPos + i] != delByte) {
            i++;
        }

        if (i > 0
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[startPos + i - 1]))) {
            throw new NumberFormatException(
                    "There is leading or trailing whitespace in the numeric field.");
        }

        final char[] chars = new char[i];
        for (int j = 0; j < i; j++) {
            final byte b = bytes[startPos + j];
            if ((b < '0' || b > '9') && b != '-' && b != '+' && b != '.' && b != 'E' && b != 'e') {
                throw new NumberFormatException();
            }
            chars[j] = (char) bytes[startPos + j];
        }
        return new BigDecimal(chars);
    }
}
