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
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link CsvInputFormat}. */
public class CsvInputFormatTest {

    private static final Path PATH = new Path("an/ignored/file/");

    // Static variables for testing the removal of \r\n to \n
    private static final String FIRST_PART = "That is the first part";

    private static final String SECOND_PART = "That is the second part";

    @Test
    public void testSplitCsvInputStreamInLargeBuffer() throws Exception {
        testSplitCsvInputStream(1024 * 1024, false, false);
        testSplitCsvInputStream(1024 * 1024, false, true);
    }

    @Test
    public void testSplitCsvInputStreamInSmallBuffer() throws Exception {
        testSplitCsvInputStream(2, false, false);
        testSplitCsvInputStream(1024 * 1024, false, true);
    }

    private void testSplitCsvInputStream(int bufferSize, boolean failAtStart, boolean compressed)
            throws Exception {
        final String fileContent =
                "this is|1|2.0|\n" + "a test|3|4.0|\n" + "#next|5|6.0|\n" + "asdadas|5|30.0|\n";

        // create temporary file with 3 blocks
        final File tempFile;

        if (compressed) {
            tempFile = File.createTempFile("TextInputFormatTest", ".compressed");
            TextInputFormat.registerInflaterInputStreamFactory(
                    "compressed",
                    new InflaterInputStreamFactory<InputStream>() {
                        @Override
                        public InputStream create(InputStream in) {
                            return in;
                        }

                        @Override
                        public Collection<String> getCommonFileExtensions() {
                            return Collections.singletonList("compressed");
                        }
                    });
        } else {
            tempFile = File.createTempFile("input-stream-decoration-test", ".tmp");
        }
        tempFile.deleteOnExit();

        try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
            fileOutputStream.write(fileContent.getBytes(ConfigConstants.DEFAULT_CHARSET));
        }

        // fix the number of blocks and the size of each one.
        final int noOfBlocks = 3;

        final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo =
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
        CsvInputFormat<Tuple3<String, Integer, Double>> format =
                new TupleCsvInputFormat<>(new Path(tempFile.toURI()), "\n", "|", typeInfo);
        format.setLenient(true);
        format.setBufferSize(bufferSize);

        final Configuration config = new Configuration();
        format.configure(config);

        long[] offsetsAfterRecord = new long[] {15, 29, 42, 58};
        long[] offsetAtEndOfSplit = new long[] {20, 40, 58};
        int recordCounter = 0;
        int splitCounter = 0;

        FileInputSplit[] inputSplits = format.createInputSplits(noOfBlocks);
        Tuple3<String, Integer, Double> result = new Tuple3<>();

        for (FileInputSplit inputSplit : inputSplits) {
            if (!compressed) {
                assertEquals(
                        inputSplit.getStart() + inputSplit.getLength(),
                        offsetAtEndOfSplit[splitCounter]);
            }
            splitCounter++;

            format.open(inputSplit);
            format.reopen(inputSplit, format.getCurrentState());

            while (!format.reachedEnd()) {
                if ((result = format.nextRecord(result)) != null) {
                    assertEquals(
                            (long) format.getCurrentState(), offsetsAfterRecord[recordCounter]);
                    recordCounter++;

                    if (recordCounter == 1) {
                        assertNotNull(result);
                        assertEquals("this is", result.f0);
                        assertEquals(Integer.valueOf(1), result.f1);
                        assertEquals(new Double(2.0), result.f2);
                        assertEquals((long) format.getCurrentState(), 15);
                    } else if (recordCounter == 2) {
                        assertNotNull(result);
                        assertEquals("a test", result.f0);
                        assertEquals(Integer.valueOf(3), result.f1);
                        assertEquals(new Double(4.0), result.f2);
                        assertEquals((long) format.getCurrentState(), 29);
                    } else if (recordCounter == 3) {
                        assertNotNull(result);
                        assertEquals("#next", result.f0);
                        assertEquals(Integer.valueOf(5), result.f1);
                        assertEquals(new Double(6.0), result.f2);
                        assertEquals((long) format.getCurrentState(), 42);
                    } else {
                        assertNotNull(result);
                        assertEquals("asdadas", result.f0);
                        assertEquals(new Integer(5), result.f1);
                        assertEquals(new Double(30.0), result.f2);
                        assertEquals((long) format.getCurrentState(), 58);
                    }

                    // simulate checkpoint
                    Long state = format.getCurrentState();
                    long offsetToRestore = state;

                    // create a new format
                    format =
                            new TupleCsvInputFormat<>(
                                    new Path(tempFile.toURI()), "\n", "|", typeInfo);
                    format.setLenient(true);
                    format.setBufferSize(bufferSize);
                    format.configure(config);

                    // simulate the restore operation.
                    format.reopen(inputSplit, offsetToRestore);
                } else {
                    result = new Tuple3<>();
                }
            }
            format.close();
        }
        Assert.assertEquals(4, recordCounter);
    }

    @Test
    public void ignoreInvalidLinesAndGetOffsetInLargeBuffer() {
        ignoreInvalidLines(1024 * 1024);
    }

    @Test
    public void ignoreInvalidLinesAndGetOffsetInSmallBuffer() {
        ignoreInvalidLines(2);
    }

    private void ignoreInvalidLines(int bufferSize) {
        try {
            final String fileContent =
                    "#description of the data\n"
                            + "header1|header2|header3|\n"
                            + "this is|1|2.0|\n"
                            + "//a comment\n"
                            + "a test|3|4.0|\n"
                            + "#next|5|6.0|\n"
                            + "asdasdas";

            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
            final CsvInputFormat<Tuple3<String, Integer, Double>> format =
                    new TupleCsvInputFormat<Tuple3<String, Integer, Double>>(
                            PATH, "\n", "|", typeInfo);
            format.setLenient(true);
            format.setBufferSize(bufferSize);

            final Configuration parameters = new Configuration();
            format.configure(parameters);
            format.open(split);

            Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();
            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("this is", result.f0);
            assertEquals(Integer.valueOf(1), result.f1);
            assertEquals(new Double(2.0), result.f2);
            assertEquals((long) format.getCurrentState(), 65);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("a test", result.f0);
            assertEquals(Integer.valueOf(3), result.f1);
            assertEquals(new Double(4.0), result.f2);
            assertEquals((long) format.getCurrentState(), 91);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("#next", result.f0);
            assertEquals(Integer.valueOf(5), result.f1);
            assertEquals(new Double(6.0), result.f2);
            assertEquals((long) format.getCurrentState(), 104);

            result = format.nextRecord(result);
            assertNull(result);
            assertEquals(fileContent.length(), (long) format.getCurrentState());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void ignoreSingleCharPrefixComments() {
        try {
            final String fileContent =
                    "#description of the data\n"
                            + "#successive commented line\n"
                            + "this is|1|2.0|\n"
                            + "a test|3|4.0|\n"
                            + "#next|5|6.0|\n";

            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
            final CsvInputFormat<Tuple3<String, Integer, Double>> format =
                    new TupleCsvInputFormat<Tuple3<String, Integer, Double>>(
                            PATH, "\n", "|", typeInfo);
            format.setCommentPrefix("#");

            final Configuration parameters = new Configuration();
            format.configure(parameters);
            format.open(split);

            Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("this is", result.f0);
            assertEquals(Integer.valueOf(1), result.f1);
            assertEquals(new Double(2.0), result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("a test", result.f0);
            assertEquals(Integer.valueOf(3), result.f1);
            assertEquals(new Double(4.0), result.f2);

            result = format.nextRecord(result);
            assertNull(result);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void ignoreMultiCharPrefixComments() {
        try {

            final String fileContent =
                    "//description of the data\n"
                            + "//successive commented line\n"
                            + "this is|1|2.0|\n"
                            + "a test|3|4.0|\n"
                            + "//next|5|6.0|\n";

            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, Integer, Double>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, Double.class);
            final CsvInputFormat<Tuple3<String, Integer, Double>> format =
                    new TupleCsvInputFormat<Tuple3<String, Integer, Double>>(
                            PATH, "\n", "|", typeInfo);
            format.setCommentPrefix("//");

            final Configuration parameters = new Configuration();
            format.configure(parameters);
            format.open(split);

            Tuple3<String, Integer, Double> result = new Tuple3<String, Integer, Double>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("this is", result.f0);
            assertEquals(Integer.valueOf(1), result.f1);
            assertEquals(new Double(2.0), result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("a test", result.f0);
            assertEquals(Integer.valueOf(3), result.f1);
            assertEquals(new Double(4.0), result.f2);

            result = format.nextRecord(result);
            assertNull(result);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void readStringFields() {
        try {
            final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, String, String>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
            final CsvInputFormat<Tuple3<String, String, String>> format =
                    new TupleCsvInputFormat<Tuple3<String, String, String>>(
                            PATH, "\n", "|", typeInfo);

            final Configuration parameters = new Configuration();
            format.configure(parameters);
            format.open(split);

            Tuple3<String, String, String> result = new Tuple3<String, String, String>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("abc", result.f0);
            assertEquals("def", result.f1);
            assertEquals("ghijk", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("abc", result.f0);
            assertEquals("", result.f1);
            assertEquals("hhg", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("", result.f0);
            assertEquals("", result.f1);
            assertEquals("", result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void readMixedQuotedStringFields() {
        try {
            final String fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, String, String>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
            final CsvInputFormat<Tuple3<String, String, String>> format =
                    new TupleCsvInputFormat<Tuple3<String, String, String>>(
                            PATH, "\n", "|", typeInfo);

            final Configuration parameters = new Configuration();
            format.enableQuotedStringParsing('@');
            format.configure(parameters);
            format.open(split);

            Tuple3<String, String, String> result = new Tuple3<String, String, String>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("a|b|c", result.f0);
            assertEquals("def", result.f1);
            assertEquals("ghijk", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("abc", result.f0);
            assertEquals("", result.f1);
            assertEquals("|hhg", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("", result.f0);
            assertEquals("", result.f1);
            assertEquals("", result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void readStringFieldsWithTrailingDelimiters() {
        try {
            final String fileContent = "abc|-def|-ghijk\nabc|-|-hhg\n|-|-|-\n";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<String, String, String>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
            final CsvInputFormat<Tuple3<String, String, String>> format =
                    new TupleCsvInputFormat<Tuple3<String, String, String>>(PATH, typeInfo);

            format.setFieldDelimiter("|-");

            format.configure(new Configuration());
            format.open(split);

            Tuple3<String, String, String> result = new Tuple3<String, String, String>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("abc", result.f0);
            assertEquals("def", result.f1);
            assertEquals("ghijk", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("abc", result.f0);
            assertEquals("", result.f1);
            assertEquals("hhg", result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals("", result.f0);
            assertEquals("", result.f1);
            assertEquals("", result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testTailingEmptyFields() throws Exception {
        final String fileContent =
                "aa,bb,cc\n"
                        + // ok
                        "aa,bb,\n"
                        + // the last field is empty
                        "aa,,\n"
                        + // the last two fields are empty
                        ",,\n"
                        + // all fields are empty
                        "aa,bb"; // row too short
        final FileInputSplit split = createTempFile(fileContent);

        final TupleTypeInfo<Tuple3<String, String, String>> typeInfo =
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class);
        final CsvInputFormat<Tuple3<String, String, String>> format =
                new TupleCsvInputFormat<Tuple3<String, String, String>>(PATH, typeInfo);

        format.setFieldDelimiter(",");

        format.configure(new Configuration());
        format.open(split);

        Tuple3<String, String, String> result = new Tuple3<String, String, String>();

        result = format.nextRecord(result);
        assertNotNull(result);
        assertEquals("aa", result.f0);
        assertEquals("bb", result.f1);
        assertEquals("cc", result.f2);

        result = format.nextRecord(result);
        assertNotNull(result);
        assertEquals("aa", result.f0);
        assertEquals("bb", result.f1);
        assertEquals("", result.f2);

        result = format.nextRecord(result);
        assertNotNull(result);
        assertEquals("aa", result.f0);
        assertEquals("", result.f1);
        assertEquals("", result.f2);

        result = format.nextRecord(result);
        assertNotNull(result);
        assertEquals("", result.f0);
        assertEquals("", result.f1);
        assertEquals("", result.f2);

        try {
            format.nextRecord(result);
            fail("Parse Exception was not thrown! (Row too short)");
        } catch (ParseException e) {
        }
    }

    @Test
    public void testIntegerFields() throws IOException {
        try {
            final String fileContent = "111|222|333|444|555\n666|777|888|999|000|\n";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Integer.class,
                            Integer.class,
                            Integer.class,
                            Integer.class,
                            Integer.class);
            final CsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>> format =
                    new TupleCsvInputFormat<Tuple5<Integer, Integer, Integer, Integer, Integer>>(
                            PATH, typeInfo);

            format.setFieldDelimiter("|");

            format.configure(new Configuration());
            format.open(split);

            Tuple5<Integer, Integer, Integer, Integer, Integer> result =
                    new Tuple5<Integer, Integer, Integer, Integer, Integer>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(111), result.f0);
            assertEquals(Integer.valueOf(222), result.f1);
            assertEquals(Integer.valueOf(333), result.f2);
            assertEquals(Integer.valueOf(444), result.f3);
            assertEquals(Integer.valueOf(555), result.f4);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(666), result.f0);
            assertEquals(Integer.valueOf(777), result.f1);
            assertEquals(Integer.valueOf(888), result.f2);
            assertEquals(Integer.valueOf(999), result.f3);
            assertEquals(Integer.valueOf(000), result.f4);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testEmptyFields() throws IOException {
        try {
            final String fileContent =
                    "|0|0|0|0|0|\n"
                            + "1||1|1|1|1|\n"
                            + "2|2||2|2|2|\n"
                            + "3|3|3| |3|3|\n"
                            + "4|4|4|4||4|\n"
                            + "5|5|5|5|5||\n";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple6<Short, Integer, Long, Float, Double, Byte>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Short.class,
                            Integer.class,
                            Long.class,
                            Float.class,
                            Double.class,
                            Byte.class);
            final CsvInputFormat<Tuple6<Short, Integer, Long, Float, Double, Byte>> format =
                    new TupleCsvInputFormat<Tuple6<Short, Integer, Long, Float, Double, Byte>>(
                            PATH, typeInfo);

            format.setFieldDelimiter("|");

            format.configure(new Configuration());
            format.open(split);

            Tuple6<Short, Integer, Long, Float, Double, Byte> result =
                    new Tuple6<Short, Integer, Long, Float, Double, Byte>();

            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (ShortParser)");
            } catch (ParseException e) {
            }
            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (IntegerParser)");
            } catch (ParseException e) {
            }
            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (LongParser)");
            } catch (ParseException e) {
            }
            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (FloatParser)");
            } catch (ParseException e) {
            }
            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (DoubleParser)");
            } catch (ParseException e) {
            }
            try {
                result = format.nextRecord(result);
                fail("Empty String Parse Exception was not thrown! (ByteParser)");
            } catch (ParseException e) {
            }

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testDoubleFields() throws IOException {
        try {
            final String fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple5<Double, Double, Double, Double, Double>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Double.class, Double.class, Double.class, Double.class, Double.class);
            final CsvInputFormat<Tuple5<Double, Double, Double, Double, Double>> format =
                    new TupleCsvInputFormat<Tuple5<Double, Double, Double, Double, Double>>(
                            PATH, typeInfo);

            format.setFieldDelimiter("|");

            format.configure(new Configuration());
            format.open(split);

            Tuple5<Double, Double, Double, Double, Double> result =
                    new Tuple5<Double, Double, Double, Double, Double>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Double.valueOf(11.1), result.f0);
            assertEquals(Double.valueOf(22.2), result.f1);
            assertEquals(Double.valueOf(33.3), result.f2);
            assertEquals(Double.valueOf(44.4), result.f3);
            assertEquals(Double.valueOf(55.5), result.f4);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Double.valueOf(66.6), result.f0);
            assertEquals(Double.valueOf(77.7), result.f1);
            assertEquals(Double.valueOf(88.8), result.f2);
            assertEquals(Double.valueOf(99.9), result.f3);
            assertEquals(Double.valueOf(00.0), result.f4);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testReadFirstN() throws IOException {
        try {
            final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class);
            final CsvInputFormat<Tuple2<Integer, Integer>> format =
                    new TupleCsvInputFormat<Tuple2<Integer, Integer>>(PATH, typeInfo);

            format.setFieldDelimiter("|");

            format.configure(new Configuration());
            format.open(split);

            Tuple2<Integer, Integer> result = new Tuple2<Integer, Integer>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(111), result.f0);
            assertEquals(Integer.valueOf(222), result.f1);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(666), result.f0);
            assertEquals(Integer.valueOf(777), result.f1);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testReadSparseWithNullFieldsForTypes() throws IOException {
        try {
            final String fileContent =
                    "111|x|222|x|333|x|444|x|555|x|666|x|777|x|888|x|999|x|000|x|\n"
                            + "000|x|999|x|888|x|777|x|666|x|555|x|444|x|333|x|222|x|111|x|";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Integer.class, Integer.class, Integer.class);
            final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format =
                    new TupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(
                            PATH,
                            typeInfo,
                            new boolean[] {true, false, false, true, false, false, false, true});

            format.setFieldDelimiter("|x|");

            format.configure(new Configuration());
            format.open(split);

            Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(111), result.f0);
            assertEquals(Integer.valueOf(444), result.f1);
            assertEquals(Integer.valueOf(888), result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(000), result.f0);
            assertEquals(Integer.valueOf(777), result.f1);
            assertEquals(Integer.valueOf(333), result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testReadSparseWithPositionSetter() throws IOException {
        try {
            final String fileContent =
                    "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Integer.class, Integer.class, Integer.class);
            final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format =
                    new TupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(
                            PATH, typeInfo, new int[] {0, 3, 7});

            format.setFieldDelimiter("|");

            format.configure(new Configuration());
            format.open(split);

            Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(111), result.f0);
            assertEquals(Integer.valueOf(444), result.f1);
            assertEquals(Integer.valueOf(888), result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(000), result.f0);
            assertEquals(Integer.valueOf(777), result.f1);
            assertEquals(Integer.valueOf(333), result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testReadSparseWithMask() throws IOException {
        try {
            final String fileContent =
                    "111&&222&&333&&444&&555&&666&&777&&888&&999&&000&&\n"
                            + "000&&999&&888&&777&&666&&555&&444&&333&&222&&111&&";
            final FileInputSplit split = createTempFile(fileContent);

            final TupleTypeInfo<Tuple3<Integer, Integer, Integer>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(
                            Integer.class, Integer.class, Integer.class);
            final CsvInputFormat<Tuple3<Integer, Integer, Integer>> format =
                    new TupleCsvInputFormat<Tuple3<Integer, Integer, Integer>>(
                            PATH,
                            typeInfo,
                            new boolean[] {true, false, false, true, false, false, false, true});

            format.setFieldDelimiter("&&");

            format.configure(new Configuration());
            format.open(split);

            Tuple3<Integer, Integer, Integer> result = new Tuple3<Integer, Integer, Integer>();

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(111), result.f0);
            assertEquals(Integer.valueOf(444), result.f1);
            assertEquals(Integer.valueOf(888), result.f2);

            result = format.nextRecord(result);
            assertNotNull(result);
            assertEquals(Integer.valueOf(000), result.f0);
            assertEquals(Integer.valueOf(777), result.f1);
            assertEquals(Integer.valueOf(333), result.f2);

            result = format.nextRecord(result);
            assertNull(result);
            assertTrue(format.reachedEnd());
        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Test
    public void testParseStringErrors() throws Exception {
        StringParser stringParser = new StringParser();
        stringParser.enableQuotedStringParsing((byte) '"');

        Object[][] failures = {
            {"\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING},
            {"\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING}
        };

        for (Object[] failure : failures) {
            String input = (String) failure[0];

            int result =
                    stringParser.parseField(
                            input.getBytes(ConfigConstants.DEFAULT_CHARSET),
                            0,
                            input.length(),
                            new byte[] {'|'},
                            null);

            assertThat(result, is(-1));
            assertThat(stringParser.getErrorState(), is(failure[1]));
        }
    }

    // Test disabled because we do not support double-quote escaped quotes right now.
    // @Test
    public void testParserCorrectness() throws Exception {
        // RFC 4180 Compliance Test content
        // Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
        final String fileContent =
                "Year,Make,Model,Description,Price\n"
                        + "1997,Ford,E350,\"ac, abs, moon\",3000.00\n"
                        + "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n"
                        + "1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n"
                        + "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n"
                        + ",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00";

        final FileInputSplit split = createTempFile(fileContent);

        final TupleTypeInfo<Tuple5<Integer, String, String, String, Double>> typeInfo =
                TupleTypeInfo.getBasicTupleTypeInfo(
                        Integer.class, String.class, String.class, String.class, Double.class);
        final CsvInputFormat<Tuple5<Integer, String, String, String, Double>> format =
                new TupleCsvInputFormat<Tuple5<Integer, String, String, String, Double>>(
                        PATH, typeInfo);

        format.setSkipFirstLineAsHeader(true);
        format.setFieldDelimiter(",");

        format.configure(new Configuration());
        format.open(split);

        Tuple5<Integer, String, String, String, Double> result =
                new Tuple5<Integer, String, String, String, Double>();

        @SuppressWarnings("unchecked")
        Tuple5<Integer, String, String, String, Double>[] expectedLines =
                new Tuple5[] {
                    new Tuple5<Integer, String, String, String, Double>(
                            1997, "Ford", "E350", "ac, abs, moon", 3000.0),
                    new Tuple5<Integer, String, String, String, Double>(
                            1999, "Chevy", "Venture \"Extended Edition\"", "", 4900.0),
                    new Tuple5<Integer, String, String, String, Double>(
                            1996,
                            "Jeep",
                            "Grand Cherokee",
                            "MUST SELL! air, moon roof, loaded",
                            4799.00),
                    new Tuple5<Integer, String, String, String, Double>(
                            1999, "Chevy", "Venture \"Extended Edition, Very Large\"", "", 5000.00),
                    new Tuple5<Integer, String, String, String, Double>(
                            0, "", "Venture \"Extended Edition\"", "", 4900.0)
                };

        try {
            for (Tuple5<Integer, String, String, String, Double> expected : expectedLines) {
                result = format.nextRecord(result);
                assertEquals(expected, result);
            }

            assertNull(format.nextRecord(result));
            assertTrue(format.reachedEnd());

        } catch (Exception ex) {
            fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    private FileInputSplit createTempFile(String content) throws IOException {
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

    @Test
    public void testWindowsLineEndRemoval() {

        // Check typical use case -- linux file is correct and it is set up to linux (\n)
        this.testRemovingTrailingCR("\n", "\n");

        // Check typical windows case -- windows file endings and file has windows file endings set
        // up
        this.testRemovingTrailingCR("\r\n", "\r\n");

        // Check problematic case windows file -- windows file endings (\r\n) but linux line endings
        // (\n) set up
        this.testRemovingTrailingCR("\r\n", "\n");

        // Check problematic case linux file -- linux file endings (\n) but windows file endings set
        // up (\r\n)
        // Specific setup for windows line endings will expect \r\n because it has to be set up and
        // is not standard.
    }

    private void testRemovingTrailingCR(String lineBreakerInFile, String lineBreakerSetup) {
        File tempFile = null;

        String fileContent =
                CsvInputFormatTest.FIRST_PART
                        + lineBreakerInFile
                        + CsvInputFormatTest.SECOND_PART
                        + lineBreakerInFile;

        try {
            // create input file
            tempFile = File.createTempFile("CsvInputFormatTest", "tmp");
            tempFile.deleteOnExit();
            tempFile.setWritable(true);

            OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
            wrt.write(fileContent);
            wrt.close();

            final TupleTypeInfo<Tuple1<String>> typeInfo =
                    TupleTypeInfo.getBasicTupleTypeInfo(String.class);
            final CsvInputFormat<Tuple1<String>> inputFormat =
                    new TupleCsvInputFormat<Tuple1<String>>(
                            new Path(tempFile.toURI().toString()), typeInfo);

            Configuration parameters = new Configuration();
            inputFormat.configure(parameters);

            inputFormat.setDelimiter(lineBreakerSetup);

            FileInputSplit[] splits = inputFormat.createInputSplits(1);

            inputFormat.open(splits[0]);

            Tuple1<String> result = inputFormat.nextRecord(new Tuple1<String>());

            assertNotNull("Expecting to not return null", result);

            assertEquals(FIRST_PART, result.f0);

            result = inputFormat.nextRecord(result);

            assertNotNull("Expecting to not return null", result);
            assertEquals(SECOND_PART, result.f0);

        } catch (Throwable t) {
            System.err.println("test failed with exception: " + t.getMessage());
            t.printStackTrace(System.err);
            fail("Test erroneous");
        }
    }

    private void validatePojoItem(CsvInputFormat<PojoItem> format) throws Exception {
        PojoItem item = new PojoItem();

        format.nextRecord(item);

        assertEquals(123, item.field1);
        assertEquals("AAA", item.field2);
        assertEquals(Double.valueOf(3.123), item.field3);
        assertEquals("BBB", item.field4);

        format.nextRecord(item);

        assertEquals(456, item.field1);
        assertEquals("BBB", item.field2);
        assertEquals(Double.valueOf(1.123), item.field3);
        assertEquals("AAA", item.field4);
    }

    @Test
    public void testPojoType() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write("123,AAA,3.123,BBB\n");
        wrt.write("456,BBB,1.123,AAA\n");
        wrt.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PojoItem> typeInfo =
                (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
        CsvInputFormat<PojoItem> inputFormat =
                new PojoCsvInputFormat<PojoItem>(new Path(tempFile.toURI().toString()), typeInfo);

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        validatePojoItem(inputFormat);
    }

    @Test
    public void testPojoTypeWithPrivateField() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write("123,AAA,3.123,BBB\n");
        wrt.write("456,BBB,1.123,AAA\n");
        wrt.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PrivatePojoItem> typeInfo =
                (PojoTypeInfo<PrivatePojoItem>) TypeExtractor.createTypeInfo(PrivatePojoItem.class);
        CsvInputFormat<PrivatePojoItem> inputFormat =
                new PojoCsvInputFormat<PrivatePojoItem>(
                        new Path(tempFile.toURI().toString()), typeInfo);

        inputFormat.configure(new Configuration());

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        inputFormat.open(splits[0]);

        PrivatePojoItem item = new PrivatePojoItem();
        inputFormat.nextRecord(item);

        assertEquals(123, item.field1);
        assertEquals("AAA", item.field2);
        assertEquals(Double.valueOf(3.123), item.field3);
        assertEquals("BBB", item.field4);

        inputFormat.nextRecord(item);

        assertEquals(456, item.field1);
        assertEquals("BBB", item.field2);
        assertEquals(Double.valueOf(1.123), item.field3);
        assertEquals("AAA", item.field4);
    }

    @Test
    public void testPojoTypeWithTrailingEmptyFields() throws Exception {
        final String fileContent = "123,,3.123,,\n456,BBB,3.23,,";
        final FileInputSplit split = createTempFile(fileContent);

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PrivatePojoItem> typeInfo =
                (PojoTypeInfo<PrivatePojoItem>) TypeExtractor.createTypeInfo(PrivatePojoItem.class);
        CsvInputFormat<PrivatePojoItem> inputFormat =
                new PojoCsvInputFormat<PrivatePojoItem>(PATH, typeInfo);

        inputFormat.configure(new Configuration());
        inputFormat.open(split);

        PrivatePojoItem item = new PrivatePojoItem();
        inputFormat.nextRecord(item);

        assertEquals(123, item.field1);
        assertEquals("", item.field2);
        assertEquals(Double.valueOf(3.123), item.field3);
        assertEquals("", item.field4);

        inputFormat.nextRecord(item);

        assertEquals(456, item.field1);
        assertEquals("BBB", item.field2);
        assertEquals(Double.valueOf(3.23), item.field3);
        assertEquals("", item.field4);
    }

    @Test
    public void testPojoTypeWithMappingInformation() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write("123,3.123,AAA,BBB\n");
        wrt.write("456,1.123,BBB,AAA\n");
        wrt.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PojoItem> typeInfo =
                (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
        CsvInputFormat<PojoItem> inputFormat =
                new PojoCsvInputFormat<PojoItem>(
                        new Path(tempFile.toURI().toString()),
                        typeInfo,
                        new String[] {"field1", "field3", "field2", "field4"});

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        validatePojoItem(inputFormat);
    }

    @Test
    public void testPojoTypeWithPartialFieldInCSV() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write("123,NODATA,AAA,NODATA,3.123,BBB\n");
        wrt.write("456,NODATA,BBB,NODATA,1.123,AAA\n");
        wrt.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PojoItem> typeInfo =
                (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
        CsvInputFormat<PojoItem> inputFormat =
                new PojoCsvInputFormat<PojoItem>(
                        new Path(tempFile.toURI().toString()),
                        typeInfo,
                        new boolean[] {true, false, true, false, true, true});

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        validatePojoItem(inputFormat);
    }

    @Test
    public void testPojoTypeWithMappingInfoAndPartialField() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile));
        wrt.write("123,3.123,AAA,BBB\n");
        wrt.write("456,1.123,BBB,AAA\n");
        wrt.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PojoItem> typeInfo =
                (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);
        CsvInputFormat<PojoItem> inputFormat =
                new PojoCsvInputFormat<PojoItem>(
                        new Path(tempFile.toURI().toString()),
                        typeInfo,
                        new String[] {"field1", "field4"},
                        new boolean[] {true, false, false, true});

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        PojoItem item = new PojoItem();
        inputFormat.nextRecord(item);

        assertEquals(123, item.field1);
        assertEquals("BBB", item.field4);
    }

    @Test
    public void testPojoTypeWithInvalidFieldMapping() throws Exception {
        File tempFile = File.createTempFile("CsvReaderPojoType", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        @SuppressWarnings("unchecked")
        PojoTypeInfo<PojoItem> typeInfo =
                (PojoTypeInfo<PojoItem>) TypeExtractor.createTypeInfo(PojoItem.class);

        try {
            new PojoCsvInputFormat<PojoItem>(
                    new Path(tempFile.toURI().toString()),
                    typeInfo,
                    new String[] {"field1", "field2"});
            fail("The number of POJO fields cannot be same as that of selected CSV fields");
        } catch (IllegalArgumentException e) {
            // success
        }

        try {
            new PojoCsvInputFormat<PojoItem>(
                    new Path(tempFile.toURI().toString()),
                    typeInfo,
                    new String[] {"field1", "field2", null, "field4"});
            fail("Fields mapping cannot contain null.");
        } catch (NullPointerException e) {
            // success
        }

        try {
            new PojoCsvInputFormat<PojoItem>(
                    new Path(tempFile.toURI().toString()),
                    typeInfo,
                    new String[] {"field1", "field2", "field3", "field5"});
            fail("Invalid field name");
        } catch (IllegalArgumentException e) {
            // success
        }
    }

    @Test
    public void testQuotedStringParsingWithIncludeFields() throws Exception {
        final String fileContent =
                "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|"
                        + "\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"blubb\"";

        final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
        writer.write(fileContent);
        writer.close();

        TupleTypeInfo<Tuple2<String, String>> typeInfo =
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
        CsvInputFormat<Tuple2<String, String>> inputFormat =
                new TupleCsvInputFormat<Tuple2<String, String>>(
                        new Path(tempFile.toURI().toString()),
                        typeInfo,
                        new boolean[] {true, false, true});

        inputFormat.enableQuotedStringParsing('"');
        inputFormat.setFieldDelimiter("|");
        inputFormat.setDelimiter('\n');

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        Tuple2<String, String> record = inputFormat.nextRecord(new Tuple2<String, String>());

        assertEquals("20:41:52-1-3-2015", record.f0);
        assertEquals("Blahblah <blah@blahblah.org>", record.f1);
    }

    @Test
    public void testQuotedStringParsingWithEscapedQuotes() throws Exception {
        final String fileContent = "\"\\\"Hello\\\" World\"|\"We are\\\" young\"";

        final File tempFile = File.createTempFile("CsvReaderQuotedString", "tmp");
        tempFile.deleteOnExit();
        tempFile.setWritable(true);

        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
        writer.write(fileContent);
        writer.close();

        TupleTypeInfo<Tuple2<String, String>> typeInfo =
                TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
        CsvInputFormat<Tuple2<String, String>> inputFormat =
                new TupleCsvInputFormat<>(new Path(tempFile.toURI().toString()), typeInfo);

        inputFormat.enableQuotedStringParsing('"');
        inputFormat.setFieldDelimiter("|");
        inputFormat.setDelimiter('\n');

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        Tuple2<String, String> record = inputFormat.nextRecord(new Tuple2<String, String>());

        assertEquals("\\\"Hello\\\" World", record.f0);
        assertEquals("We are\\\" young", record.f1);
    }

    /**
     * Tests that the CSV input format can deal with POJOs which are subclasses.
     *
     * @throws Exception
     */
    @Test
    public void testPojoSubclassType() throws Exception {
        final String fileContent = "t1,foobar,tweet2\nt2,barfoo,tweet2";

        final File tempFile = File.createTempFile("CsvReaderPOJOSubclass", "tmp");
        tempFile.deleteOnExit();

        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile));
        writer.write(fileContent);
        writer.close();

        @SuppressWarnings("unchecked")
        PojoTypeInfo<TwitterPOJO> typeInfo =
                (PojoTypeInfo<TwitterPOJO>) TypeExtractor.createTypeInfo(TwitterPOJO.class);
        CsvInputFormat<TwitterPOJO> inputFormat =
                new PojoCsvInputFormat<>(new Path(tempFile.toURI().toString()), typeInfo);

        inputFormat.configure(new Configuration());
        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        List<TwitterPOJO> expected = new ArrayList<>();

        for (String line : fileContent.split("\n")) {
            String[] elements = line.split(",");
            expected.add(new TwitterPOJO(elements[0], elements[1], elements[2]));
        }

        List<TwitterPOJO> actual = new ArrayList<>();

        TwitterPOJO pojo;

        while ((pojo = inputFormat.nextRecord(new TwitterPOJO())) != null) {
            actual.add(pojo);
        }

        assertEquals(expected, actual);
    }

    // --------------------------------------------------------------------------------------------
    // Custom types for testing
    // --------------------------------------------------------------------------------------------

    /** Sample test pojo. */
    public static class PojoItem {
        public int field1;
        public String field2;
        public Double field3;
        public String field4;
    }

    /** Sample test pojo with private fields. */
    public static class PrivatePojoItem {
        private int field1;
        private String field2;
        private Double field3;
        private String field4;

        public int getField1() {
            return field1;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public Double getField3() {
            return field3;
        }

        public void setField3(Double field3) {
            this.field3 = field3;
        }

        public String getField4() {
            return field4;
        }

        public void setField4(String field4) {
            this.field4 = field4;
        }
    }

    /** Sample test pojo. */
    public static class POJO {
        public String table;
        public String time;

        public POJO() {
            this("", "");
        }

        public POJO(String table, String time) {
            this.table = table;
            this.time = time;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof POJO) {
                POJO other = (POJO) obj;
                return table.equals(other.table) && time.equals(other.time);
            } else {
                return false;
            }
        }
    }

    /** Sample test pojo representing tweets. */
    public static class TwitterPOJO extends POJO {
        public String tweet;

        public TwitterPOJO() {
            this("", "", "");
        }

        public TwitterPOJO(String table, String time, String tweet) {
            super(table, time);
            this.tweet = tweet;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TwitterPOJO) {
                TwitterPOJO other = (TwitterPOJO) obj;

                return super.equals(other) && tweet.equals(other.tweet);
            } else {
                return false;
            }
        }
    }
}
