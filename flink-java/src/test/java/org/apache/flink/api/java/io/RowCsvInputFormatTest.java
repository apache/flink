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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link RowCsvInputFormat}. */
class RowCsvInputFormatTest {

    private static final Path PATH = new Path("an/ignored/file/");

    // static variables for testing the removal of \r\n to \n
    private static final String FIRST_PART = "That is the first part";
    private static final String SECOND_PART = "That is the second part";

    @Test
    void ignoreInvalidLines() throws Exception {
        String fileContent =
                "#description of the data\n"
                        + "header1|header2|header3|\n"
                        + "this is|1|2.0|\n"
                        + "//a comment\n"
                        + "a test|3|4.0|\n"
                        + "#next|5|6.0|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.setLenient(false);
        Configuration parameters = new Configuration();
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);
        try {
            result = format.nextRecord(result);
            fail("Parse Exception was not thrown! (Row too short)");
        } catch (ParseException ignored) {
        } // => ok

        try {
            result = format.nextRecord(result);
            fail("Parse Exception was not thrown! (Invalid int value)");
        } catch (ParseException ignored) {
        } // => ok

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("this is");
        assertThat(result.getField(1)).isEqualTo(1);
        assertThat(result.getField(2)).isEqualTo(2.0);

        try {
            result = format.nextRecord(result);
            fail("Parse Exception was not thrown! (Row too short)");
        } catch (ParseException ignored) {
        } // => ok

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("a test");
        assertThat(result.getField(1)).isEqualTo(3);
        assertThat(result.getField(2)).isEqualTo(4.0);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("#next");
        assertThat(result.getField(1)).isEqualTo(5);
        assertThat(result.getField(2)).isEqualTo(6.0);

        result = format.nextRecord(result);
        assertThat(result).isNull();

        // re-open with lenient = true
        format.setLenient(true);
        format.configure(parameters);
        format.open(split);

        result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("header1");
        assertThat(result.getField(1)).isNull();
        assertThat(result.getField(2)).isNull();

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("this is");
        assertThat(result.getField(1)).isEqualTo(1);
        assertThat(result.getField(2)).isEqualTo(2.0);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("a test");
        assertThat(result.getField(1)).isEqualTo(3);
        assertThat(result.getField(2)).isEqualTo(4.0);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("#next");
        assertThat(result.getField(1)).isEqualTo(5);
        assertThat(result.getField(2)).isEqualTo(6.0);
        result = format.nextRecord(result);
        assertThat(result).isNull();
    }

    @Test
    void ignoreSingleCharPrefixComments() throws Exception {
        String fileContent =
                "#description of the data\n"
                        + "#successive commented line\n"
                        + "this is|1|2.0|\n"
                        + "a test|3|4.0|\n"
                        + "#next|5|6.0|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.setCommentPrefix("#");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("this is");
        assertThat(result.getField(1)).isEqualTo(1);
        assertThat(result.getField(2)).isEqualTo(2.0);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("a test");
        assertThat(result.getField(1)).isEqualTo(3);
        assertThat(result.getField(2)).isEqualTo(4.0);

        result = format.nextRecord(result);
        assertThat(result).isNull();
    }

    @Test
    void ignoreMultiCharPrefixComments() throws Exception {
        String fileContent =
                "//description of the data\n"
                        + "//successive commented line\n"
                        + "this is|1|2.0|\n"
                        + "a test|3|4.0|\n"
                        + "//next|5|6.0|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.setCommentPrefix("//");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("this is");
        assertThat(result.getField(1)).isEqualTo(1);
        assertThat(result.getField(2)).isEqualTo(2.0);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("a test");
        assertThat(result.getField(1)).isEqualTo(3);
        assertThat(result.getField(2)).isEqualTo(4.0);
        result = format.nextRecord(result);
        assertThat(result).isNull();
    }

    @Test
    void readStringFields() throws Exception {
        String fileContent = "abc|def|ghijk\nabc||hhg\n|||\n||";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("def");
        assertThat(result.getField(2)).isEqualTo("ghijk");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("hhg");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void readMixedQuotedStringFields() throws Exception {
        String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.enableQuotedStringParsing('@');
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("a|b|c");
        assertThat(result.getField(1)).isEqualTo("def");
        assertThat(result.getField(2)).isEqualTo("ghijk");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("|hhg");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void readStringFieldsWithTrailingDelimiters() throws Exception {
        String fileContent = "abc|-def|-ghijk\nabc|-|-hhg\n|-|-|-\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.setFieldDelimiter("|-");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("def");
        assertThat(result.getField(2)).isEqualTo("ghijk");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("hhg");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testTailingEmptyFields() throws Exception {
        String fileContent =
                "abc|-def|-ghijk\n"
                        + "abc|-def|-\n"
                        + "abc|-|-\n"
                        + "|-|-|-\n"
                        + "|-|-\n"
                        + "abc|-def\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");
        format.setFieldDelimiter("|-");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("def");
        assertThat(result.getField(2)).isEqualTo("ghijk");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("def");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("abc");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo("");
        assertThat(result.getField(1)).isEqualTo("");
        assertThat(result.getField(2)).isEqualTo("");

        try {
            format.nextRecord(result);
            fail("Parse Exception was not thrown! (Row too short)");
        } catch (ParseException e) {
        }
    }

    @Test
    void testIntegerFields() throws Exception {
        String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, "\n", "|");

        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(5);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo(111);
        assertThat(result.getField(1)).isEqualTo(222);
        assertThat(result.getField(2)).isEqualTo(333);
        assertThat(result.getField(3)).isEqualTo(444);
        assertThat(result.getField(4)).isEqualTo(555);

        result = format.nextRecord(result);
        assertThat(result.getField(0)).isEqualTo(666);
        assertThat(result.getField(1)).isEqualTo(777);
        assertThat(result.getField(2)).isEqualTo(888);
        assertThat(result.getField(3)).isEqualTo(999);
        assertThat(result.getField(4)).isEqualTo(0);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testEmptyFields() throws Exception {
        String fileContent =
                ",,,,,,,,\n"
                        + ",,,,,,,\n"
                        + ",,,,,,,,\n"
                        + ",,,,,,,\n"
                        + ",,,,,,,,\n"
                        + ",,,,,,,,\n"
                        + ",,,,,,,\n"
                        + ",,,,,,,,\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.BOOLEAN_TYPE_INFO,
                    BasicTypeInfo.BYTE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.FLOAT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.SHORT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, true);
        format.setFieldDelimiter(",");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(8);
        int linesCnt = fileContent.split("\n").length;

        for (int i = 0; i < linesCnt; i++) {
            result = format.nextRecord(result);
            assertThat(result.getField(i)).isNull();
        }

        // ensure no more rows
        assertThat(format.nextRecord(result)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testDoubleFields() throws Exception {
        String fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(5);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(11.1);
        assertThat(result.getField(1)).isEqualTo(22.2);
        assertThat(result.getField(2)).isEqualTo(33.3);
        assertThat(result.getField(3)).isEqualTo(44.4);
        assertThat(result.getField(4)).isEqualTo(55.5);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(66.6);
        assertThat(result.getField(1)).isEqualTo(77.7);
        assertThat(result.getField(2)).isEqualTo(88.8);
        assertThat(result.getField(3)).isEqualTo(99.9);
        assertThat(result.getField(4)).isEqualTo(0.0);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadFirstN() throws Exception {
        String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(2);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(111);
        assertThat(result.getField(1)).isEqualTo(222);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(666);
        assertThat(result.getField(1)).isEqualTo(777);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadSparseWithNullFieldsForTypes() throws Exception {
        String fileContent =
                "111|x|222|x|333|x|444|x|555|x|666|x|777|x|888|x|999|x|000|x|\n"
                        + "000|x|999|x|888|x|777|x|666|x|555|x|444|x|333|x|222|x|111|x|";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, new int[] {0, 3, 7});
        format.setFieldDelimiter("|x|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(111);
        assertThat(result.getField(1)).isEqualTo(444);
        assertThat(result.getField(2)).isEqualTo(888);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(0);
        assertThat(result.getField(1)).isEqualTo(777);
        assertThat(result.getField(2)).isEqualTo(333);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadSparseWithPositionSetter() throws Exception {
        String fileContent =
                "111|222|333|444|555|666|777|888|999|000|\n"
                        + "000|999|888|777|666|555|444|333|222|111|";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, new int[] {0, 3, 7});
        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);
        result = format.nextRecord(result);

        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(111);
        assertThat(result.getField(1)).isEqualTo(444);
        assertThat(result.getField(2)).isEqualTo(888);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(0);
        assertThat(result.getField(1)).isEqualTo(777);
        assertThat(result.getField(2)).isEqualTo(333);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadSparseWithMask() throws Exception {
        String fileContent =
                "111&&222&&333&&444&&555&&666&&777&&888&&999&&000&&\n"
                        + "000&&999&&888&&777&&666&&555&&444&&333&&222&&111&&";

        FileInputSplit split = RowCsvInputFormatTest.createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, new int[] {0, 3, 7});
        format.setFieldDelimiter("&&");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(111);
        assertThat(result.getField(1)).isEqualTo(444);
        assertThat(result.getField(2)).isEqualTo(888);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(0);
        assertThat(result.getField(1)).isEqualTo(777);
        assertThat(result.getField(2)).isEqualTo(333);

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testParseStringErrors() {
        StringParser stringParser = new StringParser();

        stringParser.enableQuotedStringParsing((byte) '"');

        Map<String, StringParser.ParseErrorState> failures = new HashMap<>();
        failures.put(
                "\"string\" trailing",
                FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
        failures.put("\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING);

        for (Map.Entry<String, StringParser.ParseErrorState> failure : failures.entrySet()) {
            int result =
                    stringParser.parseField(
                            failure.getKey().getBytes(ConfigConstants.DEFAULT_CHARSET),
                            0,
                            failure.getKey().length(),
                            new byte[] {(byte) '|'},
                            null);
            assertThat(result).isEqualTo(-1);
            assertThat(stringParser.getErrorState()).isEqualTo(failure.getValue());
        }
    }

    @Test
    @Disabled("Test disabled because we do not support double-quote escaped quotes right now.")
    void testParserCorrectness() throws Exception {
        // RFC 4180 Compliance Test content
        // Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
        String fileContent =
                "Year,Make,Model,Description,Price\n"
                        + "1997,Ford,E350,\"ac, abs, moon\",3000.00\n"
                        + "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n"
                        + "1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n"
                        + "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n"
                        + ",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
        format.setSkipFirstLineAsHeader(true);
        format.setFieldDelimiter(",");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(5);
        Row r1 = new Row(5);
        r1.setField(0, 1997);
        r1.setField(1, "Ford");
        r1.setField(2, "E350");
        r1.setField(3, "ac, abs, moon");
        r1.setField(4, 3000.0);

        Row r2 = new Row(5);
        r2.setField(0, 1999);
        r2.setField(1, "Chevy");
        r2.setField(2, "Venture \"Extended Edition\"");
        r2.setField(3, "");
        r2.setField(4, 4900.0);

        Row r3 = new Row(5);
        r3.setField(0, 1996);
        r3.setField(1, "Jeep");
        r3.setField(2, "Grand Cherokee");
        r3.setField(3, "MUST SELL! air, moon roof, loaded");
        r3.setField(4, 4799.0);

        Row r4 = new Row(5);
        r4.setField(0, 1999);
        r4.setField(1, "Chevy");
        r4.setField(2, "Venture \"Extended Edition, Very Large\"");
        r4.setField(3, "");
        r4.setField(4, 5000.0);

        Row r5 = new Row(5);
        r5.setField(0, 0);
        r5.setField(1, "");
        r5.setField(2, "Venture \"Extended Edition\"");
        r5.setField(3, "");
        r5.setField(4, 4900.0);

        Row[] expectedLines = new Row[] {r1, r2, r3, r4, r5};
        for (Row expected : expectedLines) {
            result = format.nextRecord(result);
            assertThat(result).isEqualTo(expected);
        }
        assertThat(format.nextRecord(result)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testWindowsLineEndRemoval() throws Exception {

        // check typical use case -- linux file is correct and it is set up to linux(\n)
        testRemovingTrailingCR("\n", "\n");

        // check typical windows case -- windows file endings and file has windows file endings set
        // up
        testRemovingTrailingCR("\r\n", "\r\n");

        // check problematic case windows file -- windows file endings(\r\n)
        // but linux line endings (\n) set up
        testRemovingTrailingCR("\r\n", "\n");

        // check problematic case linux file -- linux file endings (\n)
        // but windows file endings set up (\r\n)
        // specific setup for windows line endings will expect \r\n because
        // it has to be set up and is not standard.
    }

    @Test
    void testQuotedStringParsingWithIncludeFields() throws Exception {
        String fileContent =
                "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|"
                        + "\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"blubb\"";
        File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
        writer.write(fileContent);
        writer.close();

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat inputFormat =
                new RowCsvInputFormat(
                        new Path(tempFile.toURI().toString()), fieldTypes, new int[] {0, 2});
        inputFormat.enableQuotedStringParsing('"');
        inputFormat.setFieldDelimiter("|");
        inputFormat.setDelimiter('\n');
        inputFormat.configure(new Configuration());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        inputFormat.open(splits[0]);

        Row record = inputFormat.nextRecord(new Row(2));
        assertThat(record.getField(0)).isEqualTo("20:41:52-1-3-2015");
        assertThat(record.getField(1)).isEqualTo("Blahblah <blah@blahblah.org>");
    }

    @Test
    void testQuotedStringParsingWithEscapedQuotes() throws Exception {
        String fileContent = "\"\\\"Hello\\\" World\"|\"We are\\\" young\"";
        File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
        writer.write(fileContent);
        writer.close();

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                };

        RowCsvInputFormat inputFormat =
                new RowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
        inputFormat.enableQuotedStringParsing('"');
        inputFormat.setFieldDelimiter("|");
        inputFormat.setDelimiter('\n');
        inputFormat.configure(new Configuration());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        inputFormat.open(splits[0]);

        Row record = inputFormat.nextRecord(new Row(2));
        assertThat(record.getField(0)).isEqualTo("\\\"Hello\\\" World");
        assertThat(record.getField(1)).isEqualTo("We are\\\" young");
    }

    @Test
    void testSqlTimeFields() throws Exception {
        String fileContent =
                "1990-10-14|02:42:25|1990-10-14 02:42:25.123|1990-1-4 2:2:5\n"
                        + "1990-10-14|02:42:25|1990-10-14 02:42:25.123|1990-1-4 2:2:5.3\n";

        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    SqlTimeTypeInfo.DATE,
                    SqlTimeTypeInfo.TIME,
                    SqlTimeTypeInfo.TIMESTAMP,
                    SqlTimeTypeInfo.TIMESTAMP
                };

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes);
        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(4);

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(Date.valueOf("1990-10-14"));
        assertThat(result.getField(1)).isEqualTo(Time.valueOf("02:42:25"));
        assertThat(result.getField(2)).isEqualTo(Timestamp.valueOf("1990-10-14 02:42:25.123"));
        assertThat(result.getField(3)).isEqualTo(Timestamp.valueOf("1990-01-04 02:02:05"));

        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(Date.valueOf("1990-10-14"));
        assertThat(result.getField(1)).isEqualTo(Time.valueOf("02:42:25"));
        assertThat(result.getField(2)).isEqualTo(Timestamp.valueOf("1990-10-14 02:42:25.123"));
        assertThat(result.getField(3)).isEqualTo(Timestamp.valueOf("1990-01-04 02:02:05.3"));

        result = format.nextRecord(result);
        assertThat(result).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testScanOrder() throws Exception {
        String fileContent =
                // first row
                "111|222|333|444|555|666|777|888|999|000|\n"
                        +
                        // second row
                        "000|999|888|777|666|555|444|333|222|111|";
        FileInputSplit split = createTempFile(fileContent);

        TypeInformation[] fieldTypes =
                new TypeInformation[] {
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO
                };

        int[] order = new int[] {7, 3, 0};
        RowCsvInputFormat format = new RowCsvInputFormat(PATH, fieldTypes, order);

        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(3);

        // check first row
        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(888);
        assertThat(result.getField(1)).isEqualTo(444);
        assertThat(result.getField(2)).isEqualTo(111);

        // check second row
        result = format.nextRecord(result);
        assertThat(result).isNotNull();
        assertThat(result.getField(0)).isEqualTo(333);
        assertThat(result.getField(1)).isEqualTo(777);
        assertThat(result.getField(2)).isEqualTo(0);
    }

    @Test
    void testEmptyProjection() throws Exception {
        String fileContent = "111|222|333\n" + "000|999|888";
        FileInputSplit split = createTempFile(fileContent);

        RowCsvInputFormat format = new RowCsvInputFormat(PATH, new TypeInformation[0], new int[0]);

        format.setFieldDelimiter("|");
        format.configure(new Configuration());
        format.open(split);

        Row result = new Row(0);

        // check first row
        result = format.nextRecord(result);
        assertThat(result).isNotNull();

        // check second row
        result = format.nextRecord(result);
        assertThat(result).isNotNull();
    }

    private static FileInputSplit createTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();
        OutputStreamWriter wrt =
                new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
        wrt.write(content);
        wrt.close();
        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    private static void testRemovingTrailingCR(String lineBreakerInFile, String lineBreakerSetup)
            throws IOException {
        String fileContent = FIRST_PART + lineBreakerInFile + SECOND_PART + lineBreakerInFile;

        // create input file
        File tempFile = File.createTempFile("CsvInputFormatTest", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write(fileContent);
        wrt.close();

        TypeInformation[] fieldTypes = new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO};

        RowCsvInputFormat inputFormat =
                new RowCsvInputFormat(new Path(tempFile.toURI().toString()), fieldTypes);
        inputFormat.configure(new Configuration());
        inputFormat.setDelimiter(lineBreakerSetup);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        inputFormat.open(splits[0]);

        Row result = inputFormat.nextRecord(new Row(1));
        assertThat(result).as("Expecting to not return null").isNotNull();
        assertThat(result.getField(0)).isEqualTo(FIRST_PART);

        result = inputFormat.nextRecord(result);
        assertThat(result).as("Expecting to not return null").isNotNull();
        assertThat(result.getField(0)).isEqualTo(SECOND_PART);
    }
}
