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

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** */
public abstract class ParserTestBase<T> {

    public abstract String[] getValidTestValues();

    public abstract T[] getValidTestResults();

    public abstract String[] getInvalidTestValues();

    public abstract boolean allowsEmptyField();

    public abstract FieldParser<T> getParser();

    public abstract Class<T> getTypeClass();

    @Test
    void testTest() {
        assertThat(getParser()).isNotNull();
        assertThat(getTypeClass()).isNotNull();
        assertThat(getValidTestValues()).isNotNull();
        assertThat(getValidTestResults()).isNotNull();
        assertThat(getInvalidTestValues()).isNotNull();
        assertThat(getValidTestValues()).hasSameSizeAs(getValidTestResults());
    }

    @Test
    void testGetValue() {
        FieldParser<?> parser = getParser();
        Object created = parser.createValue();

        assertThat(created).isNotNull().isInstanceOf(getTypeClass());
    }

    @Test
    void testValidStringInIsolation() {
        String[] testValues = getValidTestValues();
        T[] results = getValidTestResults();

        for (int i = 0; i < testValues.length; i++) {

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();
            FieldParser<T> parser3 = getParser();

            byte[] bytes1 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
            byte[] bytes2 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
            byte[] bytes3 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

            int numRead1 =
                    parser1.parseField(
                            bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
            int numRead2 =
                    parser2.parseField(
                            bytes2, 0, bytes2.length, new byte[] {'&', '&'}, parser2.createValue());
            int numRead3 =
                    parser3.parseField(
                            bytes3,
                            0,
                            bytes3.length,
                            new byte[] {'9', '9', '9'},
                            parser3.createValue());

            assertThat(numRead1)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            assertThat(numRead2)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            assertThat(numRead3)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);

            assertThat(bytes1).hasSize(numRead1);
            assertThat(bytes2).hasSize(numRead2);
            assertThat(bytes3).hasSize(numRead3);

            assertThat(parser1.getLastResult()).isEqualTo(results[i]);
            assertThat(parser2.getLastResult()).isEqualTo(results[i]);
            assertThat(parser3.getLastResult()).isEqualTo(results[i]);
        }
    }

    @Test
    void testValidStringInIsolationWithEndDelimiter() {
        String[] testValues = getValidTestValues();
        T[] results = getValidTestResults();

        for (int i = 0; i < testValues.length; i++) {

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();

            String testVal1 = testValues[i] + "|";
            String testVal2 = testValues[i] + "&&&&";

            byte[] bytes1 = testVal1.getBytes(ConfigConstants.DEFAULT_CHARSET);
            byte[] bytes2 = testVal2.getBytes(ConfigConstants.DEFAULT_CHARSET);

            int numRead1 =
                    parser1.parseField(
                            bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
            int numRead2 =
                    parser2.parseField(
                            bytes2,
                            0,
                            bytes2.length,
                            new byte[] {'&', '&', '&', '&'},
                            parser2.createValue());

            assertThat(numRead1)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            assertThat(numRead2)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);

            assertThat(bytes1).hasSize(numRead1);
            assertThat(bytes2).hasSize(numRead2);

            assertThat(parser1.getLastResult()).isEqualTo(results[i]);
            assertThat(parser2.getLastResult()).isEqualTo(results[i]);
        }
    }

    @Test
    void testConcatenated() {
        String[] testValues = getValidTestValues();
        T[] results = getValidTestResults();
        byte[] allBytesWithDelimiter = concatenate(testValues, new char[] {'|'}, true);
        byte[] allBytesNoDelimiterEnd = concatenate(testValues, new char[] {','}, false);

        FieldParser<T> parser1 = getParser();
        FieldParser<T> parser2 = getParser();

        T val1 = parser1.createValue();
        T val2 = parser2.createValue();

        int pos1 = 0;
        int pos2 = 0;

        for (int i = 0; i < results.length; i++) {
            pos1 =
                    parser1.parseField(
                            allBytesWithDelimiter,
                            pos1,
                            allBytesWithDelimiter.length,
                            new byte[] {'|'},
                            val1);
            pos2 =
                    parser2.parseField(
                            allBytesNoDelimiterEnd,
                            pos2,
                            allBytesNoDelimiterEnd.length,
                            new byte[] {','},
                            val2);

            assertThat(pos1)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            assertThat(pos2)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);

            assertThat(parser1.getLastResult()).isEqualTo(results[i]);
            assertThat(parser2.getLastResult()).isEqualTo(results[i]);
        }
    }

    @Test
    void testConcatenatedMultiCharDelimiter() {
        String[] testValues = getValidTestValues();
        T[] results = getValidTestResults();
        byte[] allBytesWithDelimiter =
                concatenate(testValues, new char[] {'&', '&', '&', '&'}, true);
        byte[] allBytesNoDelimiterEnd = concatenate(testValues, new char[] {'9', '9', '9'}, false);

        FieldParser<T> parser1 = getParser();
        FieldParser<T> parser2 = getParser();

        T val1 = parser1.createValue();
        T val2 = parser2.createValue();

        int pos1 = 0;
        int pos2 = 0;

        for (int i = 0; i < results.length; i++) {
            pos1 =
                    parser1.parseField(
                            allBytesWithDelimiter,
                            pos1,
                            allBytesWithDelimiter.length,
                            new byte[] {'&', '&', '&', '&'},
                            val1);
            assertThat(pos1)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            T result1 = parser1.getLastResult();
            assertThat(result1).isEqualTo(results[i]);

            pos2 =
                    parser2.parseField(
                            allBytesNoDelimiterEnd,
                            pos2,
                            allBytesNoDelimiterEnd.length,
                            new byte[] {'9', '9', '9'},
                            val2);
            assertThat(pos2)
                    .describedAs(
                            "Parser declared the valid value " + testValues[i] + " as invalid.")
                    .isNotEqualTo(-1);
            T result2 = parser2.getLastResult();
            assertThat(result2).isEqualTo(results[i]);
        }
    }

    @Test
    void testInValidStringInIsolation() {
        String[] testValues = getInvalidTestValues();

        for (String testValue : testValues) {

            FieldParser<T> parser = getParser();

            byte[] bytes = testValue.getBytes(ConfigConstants.DEFAULT_CHARSET);
            int numRead =
                    parser.parseField(
                            bytes, 0, bytes.length, new byte[] {'|'}, parser.createValue());

            assertThat(numRead)
                    .describedAs("Parser accepted the invalid value " + testValue + ".")
                    .isEqualTo(-1);
        }
    }

    @Test
    void testInValidStringsMixedIn() {
        String[] validValues = getValidTestValues();
        T[] validResults = getValidTestResults();

        String[] invalidTestValues = getInvalidTestValues();

        FieldParser<T> parser = getParser();
        T value = parser.createValue();

        for (String invalid : invalidTestValues) {

            // place an invalid string in the middle
            String[] testLine = new String[validValues.length + 1];
            int splitPoint = validValues.length / 2;
            System.arraycopy(validValues, 0, testLine, 0, splitPoint);
            testLine[splitPoint] = invalid;
            System.arraycopy(
                    validValues,
                    splitPoint,
                    testLine,
                    splitPoint + 1,
                    validValues.length - splitPoint);

            byte[] bytes = concatenate(testLine, new char[] {'%'}, true);

            // read the valid parts
            int pos = 0;
            for (int i = 0; i < splitPoint; i++) {
                pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);

                assertThat(pos)
                        .describedAs(
                                "Parser declared the valid value "
                                        + validValues[i]
                                        + " as invalid.")
                        .isNotEqualTo(-1);

                T result = parser.getLastResult();
                assertThat(result).describedAs("Parser parsed wrong.").isEqualTo(validResults[i]);
            }

            // fail on the invalid part
            pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);
            assertThat(pos)
                    .describedAs("Parser accepted the invalid value " + invalid + ".")
                    .isEqualTo(-1);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStaticParseMethod() throws IllegalAccessException, InvocationTargetException {
        Method parseMethod;
        try {
            parseMethod =
                    getParser()
                            .getClass()
                            .getMethod(
                                    "parseField", byte[].class, int.class, int.class, char.class);
        } catch (NoSuchMethodException e) {
            return;
        }

        String[] testValues = getValidTestValues();
        T[] results = getValidTestResults();

        for (int i = 0; i < testValues.length; i++) {
            byte[] bytes = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
            assertThat((T) parseMethod.invoke(null, bytes, 0, bytes.length, '|'))
                    .isEqualTo(results[i]);
        }
    }

    @Test
    void testStaticParseMethodWithInvalidValues() {
        Method parseMethod;
        try {
            parseMethod =
                    getParser()
                            .getClass()
                            .getMethod(
                                    "parseField", byte[].class, int.class, int.class, char.class);
        } catch (NoSuchMethodException e) {
            return;
        }

        String[] testValues = getInvalidTestValues();

        for (String testValue : testValues) {

            byte[] bytes = testValue.getBytes(ConfigConstants.DEFAULT_CHARSET);

            Method finalParseMethod = parseMethod;
            assertThatThrownBy(() -> finalParseMethod.invoke(null, bytes, 0, bytes.length, '|'))
                    .isInstanceOf(InvocationTargetException.class);
        }
    }

    private static byte[] concatenate(String[] values, char[] delimiter, boolean delimiterAtEnd) {
        int len = 0;
        for (String s : values) {
            len += s.length() + delimiter.length;
        }

        if (!delimiterAtEnd) {
            len -= delimiter.length;
        }

        int currPos = 0;
        byte[] result = new byte[len];

        for (int i = 0; i < values.length; i++) {
            String s = values[i];

            byte[] bytes = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            int numBytes = bytes.length;
            System.arraycopy(bytes, 0, result, currPos, numBytes);
            currPos += numBytes;

            if (delimiterAtEnd || i < values.length - 1) {
                for (char c : delimiter) result[currPos++] = (byte) c;
            }
        }

        return result;
    }

    @Test
    void testTrailingEmptyField() {
        FieldParser<T> parser = getParser();

        byte[] bytes = "||".getBytes(ConfigConstants.DEFAULT_CHARSET);

        for (int i = 0; i < 2; i++) {

            // test empty field with trailing delimiter when i = 0,
            // test empty field without trailing delimiter when i= 1.
            int numRead =
                    parser.parseField(
                            bytes, i, bytes.length, new byte[] {'|'}, parser.createValue());

            assertThat(parser.getErrorState()).isEqualTo(FieldParser.ParseErrorState.EMPTY_COLUMN);

            if (this.allowsEmptyField()) {
                assertThat(numRead).isNotEqualTo(-1);
                assertThat(numRead).isEqualTo(i + 1);
            } else {
                assertThat(numRead).isEqualTo(-1);
            }
            parser.resetParserState();
        }
    }
}
