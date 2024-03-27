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

package org.apache.flink.api.common.io;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.flink.api.common.io.DelimitedInputFormatTest.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenericCsvInputFormatTest {

    private TestCsvInputFormat format;

    // --------------------------------------------------------------------------------------------

    @BeforeEach
    void setup() {
        format = new TestCsvInputFormat();
        format.setFilePath("file:///some/file/that/will/not/be/read");
    }

    @AfterEach
    void setdown() throws Exception {
        if (this.format != null) {
            this.format.close();
        }
    }

    @Test
    void testSparseFieldArray() {

        @SuppressWarnings("unchecked")
        Class<? extends Value>[] originalTypes =
                new Class[] {
                    IntValue.class, null, null, StringValue.class, null, DoubleValue.class
                };

        format.setFieldTypesGeneric(originalTypes);
        assertThat(format.getNumberOfNonNullFields()).isEqualTo(3);
        assertThat(format.getNumberOfFieldsTotal()).isEqualTo(6);

        assertThat(format.getGenericFieldTypes()).isEqualTo(originalTypes);
    }

    @Test
    void testReadNoPosAll() throws IOException {

        final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(222);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(555);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(666);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(999);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(000);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadNoPosAllDeflate() throws IOException {

        final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
        final FileInputSplit split = createTempDeflateFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(222);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(555);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(666);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(999);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(000);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadNoPosAllGzip() throws IOException {

        final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
        final FileInputSplit split = createTempGzipFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(222);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(555);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(666);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(999);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(000);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadNoPosAllZStandard() throws IOException {

        final String fileContent = "111|222|333|444|555\n666|777|888|999|000|";
        final FileInputSplit split = createTempZStandardFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(222);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(555);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(666);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);
        assertThat(((IntValue) values[3]).getValue()).isEqualTo(999);
        assertThat(((IntValue) values[4]).getValue()).isEqualTo(000);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadNoPosFirstN() throws IOException {

        final String fileContent = "111|222|333|444|555|\n666|777|888|999|000|";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(2);

        // if this would parse all, we would get an index out of bounds exception
        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(222);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(666);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testSparseParse() throws IOException {

        final String fileContent =
                "111|222|333|444|555|666|777|888|999|000|\n"
                        + "000|999|888|777|666|555|444|333|222|111|";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, null, null, IntValue.class, null, null, null, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(3);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(000);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testLongLongLong() throws IOException {

        final String fileContent = "1,2,3\n3,2,1";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter(",");
        format.setFieldTypesGeneric(LongValue.class, LongValue.class, LongValue.class);
        format.configure(parameters);
        format.open(split);

        Value[] values = createLongValues(3);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((LongValue) values[0]).getValue()).isEqualTo(1L);
        assertThat(((LongValue) values[1]).getValue()).isEqualTo(2L);
        assertThat(((LongValue) values[2]).getValue()).isEqualTo(3L);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((LongValue) values[0]).getValue()).isEqualTo(3L);
        assertThat(((LongValue) values[1]).getValue()).isEqualTo(2L);
        assertThat(((LongValue) values[2]).getValue()).isEqualTo(1L);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSparseParseWithIndices() throws IOException {

        final String fileContent =
                "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666|555|444|333|222|111|";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldsGeneric(
                new int[] {0, 3, 7}, new Class[] {IntValue.class, IntValue.class, IntValue.class});
        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(3);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(000);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSparseParseWithIndicesMultiCharDelimiter() throws IOException {

        final String fileContent =
                "111|-|222|-|333|-|444|-|555|-|666|-|777|-|888|-|999|-|000|-|\n"
                        + "000|-|999|-|888|-|777|-|666|-|555|-|444|-|333|-|222|-|111\n"
                        + "555|-|999|-|888|-|111|-|666|-|555|-|444|-|777|-|222|-|111|-|\n"
                        + "22222|-|99999|-|8|-|99999999|-|6666666|-|5|-|4444|-|8|-|22222|-|1\n";

        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|-|");
        format.setFieldsGeneric(
                new int[] {0, 3, 7}, new Class[] {IntValue.class, IntValue.class, IntValue.class});
        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(3);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(444);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(888);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(000);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(777);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(333);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(555);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(111);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(777);

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((IntValue) values[0]).getValue()).isEqualTo(22222);
        assertThat(((IntValue) values[1]).getValue()).isEqualTo(99999999);
        assertThat(((IntValue) values[2]).getValue()).isEqualTo(8);

        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadTooShortInput() throws IOException {

        final String fileContent = "111|222|333|444\n666|777|888|999";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();
        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        assertThatThrownBy(() -> format.nextRecord(values)).isInstanceOf(ParseException.class);
    }

    @Test
    void testReadTooShortInputLenient() throws IOException {

        final String fileContent = "666|777|888|999|555\n111|222|333|444\n666|777|888|999|555";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();
        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, IntValue.class, IntValue.class, IntValue.class, IntValue.class);
        format.setLenient(true);

        format.configure(parameters);
        format.open(split);

        Value[] values = createIntValues(5);

        assertThat(format.nextRecord(values)).isNotNull(); // line okay
        assertThat(format.nextRecord(values)).isNull(); // line too short
        assertThat(format.nextRecord(values)).isNotNull(); // line okay
    }

    @Test
    void testReadInvalidContents() throws IOException {

        final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                StringValue.class, IntValue.class, StringValue.class, IntValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values =
                new Value[] {new StringValue(), new IntValue(), new StringValue(), new IntValue()};

        assertThat(format.nextRecord(values)).isNotNull();

        assertThatThrownBy(() -> format.nextRecord(values)).isInstanceOf(ParseException.class);
    }

    @Test
    void testReadInvalidContentsLenient() throws IOException {

        final String fileContent = "abc|222|def|444\nkkz|777|888|hhg";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                StringValue.class, IntValue.class, StringValue.class, IntValue.class);
        format.setLenient(true);

        format.configure(parameters);
        format.open(split);

        Value[] values =
                new Value[] {new StringValue(), new IntValue(), new StringValue(), new IntValue()};

        assertThat(format.nextRecord(values)).isNotNull();
        assertThat(format.nextRecord(values)).isNull();
    }

    @Test
    void testReadInvalidContentsLenientWithSkipping() throws IOException {

        final String fileContent =
                "abc|dfgsdf|777|444\n"
                        + // good line
                        "kkz|777|foobar|hhg\n"
                        + // wrong data type in field
                        "kkz|777foobarhhg  \n"
                        + // too short, a skipped field never ends
                        "xyx|ignored|42|\n"; // another good line
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(StringValue.class, null, IntValue.class);
        format.setLenient(true);

        format.configure(parameters);
        format.open(split);

        Value[] values = new Value[] {new StringValue(), new IntValue()};

        assertThat(format.nextRecord(values)).isNotNull();
        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.nextRecord(values)).isNull();
        assertThat(format.nextRecord(values)).isNotNull();
    }

    @Test
    void testReadWithCharset() throws IOException {
        // Unicode row fragments
        String[] records = new String[] {"\u020e\u021f", "Flink", "\u020b\u020f"};

        // Unicode delimiter
        String delimiter = "\u05c0\u05c0";

        String fileContent = StringUtils.join(records, delimiter);

        // StringValueParser does not use charset so rely on StringParser
        GenericCsvInputFormat<String[]> format =
                new GenericCsvInputFormat<String[]>() {
                    @Override
                    public String[] readRecord(
                            String[] target, byte[] bytes, int offset, int numBytes) {
                        return parseRecord(target, bytes, offset, numBytes) ? target : null;
                    }
                };
        format.setFilePath("file:///some/file/that/will/not/be/read");

        for (String charset : new String[] {"UTF-8", "UTF-16BE", "UTF-16LE"}) {
            File tempFile = File.createTempFile("test_contents", "tmp");
            tempFile.deleteOnExit();

            // write string with proper encoding
            try (Writer out =
                    new OutputStreamWriter(Files.newOutputStream(tempFile.toPath()), charset)) {
                out.write(fileContent);
            }

            FileInputSplit split =
                    new FileInputSplit(
                            0,
                            new Path(tempFile.toURI().toString()),
                            0,
                            tempFile.length(),
                            new String[] {"localhost"});

            format.setFieldDelimiter(delimiter);
            format.setFieldTypesGeneric(String.class, String.class, String.class);
            // use the same encoding to parse the file as used to read the file;
            // the field delimiter is reinterpreted when the charset is set
            format.setCharset(charset);
            format.configure(new Configuration());
            format.open(split);

            String[] values = new String[] {"", "", ""};
            values = format.nextRecord(values);

            // validate results
            assertThat(values).isNotNull();
            for (int i = 0; i < records.length; i++) {
                assertThat(values[i]).isEqualTo(records[i]);
            }

            assertThat(format.nextRecord(values)).isNull();
            assertThat(format.reachedEnd()).isTrue();
        }

        format.close();
    }

    @Test
    void readWithEmptyField() throws IOException {

        final String fileContent = "abc|def|ghijk\nabc||hhg\n|||";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(StringValue.class, StringValue.class, StringValue.class);

        format.configure(parameters);
        format.open(split);

        Value[] values = new Value[] {new StringValue(), new StringValue(), new StringValue()};

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((StringValue) values[0]).getValue()).isEqualTo("abc");
        assertThat(((StringValue) values[1]).getValue()).isEqualTo("def");
        assertThat(((StringValue) values[2]).getValue()).isEqualTo("ghijk");

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((StringValue) values[0]).getValue()).isEqualTo("abc");
        assertThat(((StringValue) values[1]).getValue()).isEqualTo("");
        assertThat(((StringValue) values[2]).getValue()).isEqualTo("hhg");

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((StringValue) values[0]).getValue()).isEqualTo("");
        assertThat(((StringValue) values[1]).getValue()).isEqualTo("");
        assertThat(((StringValue) values[2]).getValue()).isEqualTo("");
    }

    @Test
    void readWithParseQuotedStrings() throws IOException {

        final String fileContent = "\"ab\\\"c\"|\"def\"\n\"ghijk\"|\"abc\"";
        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(StringValue.class, StringValue.class);
        format.enableQuotedStringParsing('"');

        format.configure(parameters);
        format.open(split);

        Value[] values = new Value[] {new StringValue(), new StringValue()};

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((StringValue) values[0]).getValue()).isEqualTo("ab\\\"c");
        assertThat(((StringValue) values[1]).getValue()).isEqualTo("def");

        values = format.nextRecord(values);
        assertThat(values).isNotNull();
        assertThat(((StringValue) values[0]).getValue()).isEqualTo("ghijk");
        assertThat(((StringValue) values[1]).getValue()).isEqualTo("abc");
    }

    @Test
    void readWithHeaderLine() throws IOException {

        final String fileContent =
                "colname-1|colname-2|some name 3|column four|\n"
                        + "123|abc|456|def|\n"
                        + "987|xyz|654|pqr|\n";

        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, StringValue.class, IntValue.class, StringValue.class);
        format.setSkipFirstLineAsHeader(true);

        format.configure(parameters);
        format.open(split);

        Value[] values =
                new Value[] {new IntValue(), new StringValue(), new IntValue(), new StringValue()};

        // first line is skipped as header
        assertThat(format.nextRecord(values)); //  first row (= second line).isNotNull()
        assertThat(format.nextRecord(values)); // second row (= third line).isNotNull()
        assertThat(format.nextRecord(values)).isNull(); // exhausted
        assertThat(format.reachedEnd()).isTrue(); // exhausted
    }

    @Test
    void readWithHeaderLineAndInvalidIntermediate() throws IOException {

        final String fileContent =
                "colname-1|colname-2|some name 3|column four|\n"
                        + "123|abc|456|def|\n"
                        + "colname-1|colname-2|some name 3|column four|\n"
                        + // repeated header in the middle
                        "987|xyz|654|pqr|\n";

        final FileInputSplit split = createTempFile(fileContent);

        final Configuration parameters = new Configuration();

        format.setFieldDelimiter("|");
        format.setFieldTypesGeneric(
                IntValue.class, StringValue.class, IntValue.class, StringValue.class);
        format.setSkipFirstLineAsHeader(true);

        format.configure(parameters);
        format.open(split);

        Value[] values =
                new Value[] {new IntValue(), new StringValue(), new IntValue(), new StringValue()};

        // first line is skipped as header
        assertThat(format.nextRecord(values)); //  first row (= second line).isNotNull()

        assertThatThrownBy(() -> format.nextRecord(values)).isInstanceOf(ParseException.class);
    }

    private FileInputSplit createTempDeflateFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp.deflate");
        tempFile.deleteOnExit();

        DataOutputStream dos =
                new DataOutputStream(
                        new DeflaterOutputStream(Files.newOutputStream(tempFile.toPath())));
        dos.writeBytes(content);
        dos.close();

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    private FileInputSplit createTempGzipFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp.gz");
        tempFile.deleteOnExit();

        DataOutputStream dos =
                new DataOutputStream(
                        new GZIPOutputStream(Files.newOutputStream(tempFile.toPath())));
        dos.writeBytes(content);
        dos.close();

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    private FileInputSplit createTempZStandardFile(String content) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp.zst");
        tempFile.deleteOnExit();

        DataOutputStream dos =
                new DataOutputStream(
                        new ZstdCompressorOutputStream(Files.newOutputStream(tempFile.toPath())));
        dos.writeBytes(content);
        dos.close();

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    private Value[] createIntValues(int num) {
        Value[] v = new Value[num];

        for (int i = 0; i < num; i++) {
            v[i] = new IntValue();
        }

        return v;
    }

    private Value[] createLongValues(int num) {
        Value[] v = new Value[num];

        for (int i = 0; i < num; i++) {
            v[i] = new LongValue();
        }

        return v;
    }

    private static final class TestCsvInputFormat extends GenericCsvInputFormat<Value[]> {

        private static final long serialVersionUID = 2653609265252951059L;

        @Override
        public Value[] readRecord(Value[] target, byte[] bytes, int offset, int numBytes) {
            return parseRecord(target, bytes, offset, numBytes) ? target : null;
        }
    }
}
