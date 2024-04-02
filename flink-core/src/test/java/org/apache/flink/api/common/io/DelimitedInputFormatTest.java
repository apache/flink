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

import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DelimitedInputFormatTest {

    private DelimitedInputFormat<String> format;

    // --------------------------------------------------------------------------------------------

    @BeforeEach
    void setup() {
        format = new MyTextInputFormat();
        this.format.setFilePath(new Path("file:///some/file/that/will/not/be/read"));
    }

    @AfterEach
    void shutdown() throws Exception {
        if (this.format != null) {
            this.format.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    // --------------------------------------------------------------------------------------------
    @Test
    void testConfigure() {
        Configuration cfg = new Configuration();
        cfg.setString("delimited-format.delimiter", "\n");

        format.configure(cfg);
        assertThat(new String(format.getDelimiter(), format.getCharset())).isEqualTo("\n");

        cfg.setString("delimited-format.delimiter", "&-&");
        format.configure(cfg);
        assertThat(new String(format.getDelimiter(), format.getCharset())).isEqualTo("&-&");
    }

    @Test
    void testSerialization() throws Exception {
        final byte[] DELIMITER = new byte[] {1, 2, 3, 4};
        final int NUM_LINE_SAMPLES = 7;
        final int LINE_LENGTH_LIMIT = 12345;
        final int BUFFER_SIZE = 178;

        DelimitedInputFormat<String> format = new MyTextInputFormat();
        format.setDelimiter(DELIMITER);
        format.setNumLineSamples(NUM_LINE_SAMPLES);
        format.setLineLengthLimit(LINE_LENGTH_LIMIT);
        format.setBufferSize(BUFFER_SIZE);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(format);
        oos.flush();
        oos.close();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        @SuppressWarnings("unchecked")
        DelimitedInputFormat<String> deserialized = (DelimitedInputFormat<String>) ois.readObject();

        assertThat(deserialized.getNumLineSamples()).isEqualTo(NUM_LINE_SAMPLES);
        assertThat(deserialized.getLineLengthLimit()).isEqualTo(LINE_LENGTH_LIMIT);
        assertThat(deserialized.getBufferSize()).isEqualTo(BUFFER_SIZE);
        assertThat(deserialized.getDelimiter()).isEqualTo(DELIMITER);
    }

    @Test
    void testOpen() throws IOException {
        final String myString = "my mocked line 1\nmy mocked line 2\n";
        final FileInputSplit split = createTempFile(myString);

        int bufferSize = 5;
        format.setBufferSize(bufferSize);
        format.open(split);
        assertThat(format.splitStart).isZero();
        assertThat(format.splitLength).isEqualTo(myString.length() - bufferSize);
        assertThat(format.getBufferSize()).isEqualTo(bufferSize);
    }

    @Test
    void testReadWithoutTrailingDelimiter() throws IOException {
        // 2. test case
        final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
        final FileInputSplit split = createTempFile(myString);

        final Configuration parameters = new Configuration();
        // default delimiter = '\n'

        format.configure(parameters);
        format.open(split);

        String first = format.nextRecord(null);
        String second = format.nextRecord(null);

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();

        assertThat(first).isEqualTo("my key|my val$$$my key2");
        assertThat(second).isEqualTo("$$ctd.$$|my value2");

        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadWithTrailingDelimiter() throws IOException {
        // 2. test case
        final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2\n";
        final FileInputSplit split = createTempFile(myString);

        final Configuration parameters = new Configuration();
        // default delimiter = '\n'

        format.configure(parameters);
        format.open(split);

        String first = format.nextRecord(null);
        String second = format.nextRecord(null);

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();

        assertThat(first).isEqualTo("my key|my val$$$my key2");
        assertThat(second).isEqualTo("$$ctd.$$|my value2");

        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadCustomDelimiter() throws IOException {
        final String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
        final FileInputSplit split = createTempFile(myString);

        final Configuration parameters = new Configuration();

        format.setDelimiter("$$$");
        format.configure(parameters);
        format.open(split);

        String first = format.nextRecord(null);
        assertThat(first).isNotNull();
        assertThat(first).isEqualTo("my key|my val");

        String second = format.nextRecord(null);
        assertThat(second).isNotNull();
        assertThat(second).isEqualTo("my key2\n$$ctd.$$|my value2");

        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testMultiCharDelimiter() throws IOException {
        final String myString = "www112xx1123yyy11123zzzzz1123";
        final FileInputSplit split = createTempFile(myString);

        final Configuration parameters = new Configuration();

        format.setDelimiter("1123");
        format.configure(parameters);
        format.open(split);

        String first = format.nextRecord(null);
        assertThat(first).isNotNull();
        assertThat(first).isEqualTo("www112xx");

        String second = format.nextRecord(null);
        assertThat(second).isNotNull();
        assertThat(second).isEqualTo("yyy1");

        String third = format.nextRecord(null);
        assertThat(third).isNotNull();
        assertThat(third).isEqualTo("zzzzz");

        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
    }

    @Test
    void testReadCustomDelimiterWithCharset() throws IOException {
        // Unicode row fragments
        String[] records =
                new String[] {
                    "\u020e\u021f\u05c0\u020b\u020f", "Apache", "\nFlink", "\u0000", "\u05c0"
                };

        // Unicode delimiter
        String delimiter = "\u05c0\u05c0";

        String fileContent = StringUtils.join(records, delimiter);

        for (final String charset : new String[] {"UTF-8", "UTF-16BE", "UTF-16LE"}) {
            // use charset when instantiating the record String
            DelimitedInputFormat<String> format =
                    new DelimitedInputFormat<String>() {
                        @Override
                        public String readRecord(
                                String reuse, byte[] bytes, int offset, int numBytes)
                                throws IOException {
                            return new String(bytes, offset, numBytes, charset);
                        }
                    };
            format.setFilePath("file:///some/file/that/will/not/be/read");

            final FileInputSplit split = createTempFile(fileContent, charset);

            format.setDelimiter(delimiter);
            // use the same encoding to parse the file as used to read the file;
            // the delimiter is reinterpreted when the charset is set
            format.setCharset(charset);
            format.configure(new Configuration());
            format.open(split);

            for (String record : records) {
                String value = format.nextRecord(null);
                assertThat(value).isEqualTo(record);
            }

            assertThat(format.nextRecord(null)).isNull();
            assertThat(format.reachedEnd()).isTrue();
        }
    }

    /**
     * Tests that the records are read correctly when the split boundary is in the middle of a
     * record.
     */
    @Test
    void testReadOverSplitBoundariesUnaligned() throws IOException {
        final String myString = "value1\nvalue2\nvalue3";
        final FileInputSplit split = createTempFile(myString);

        FileInputSplit split1 =
                new FileInputSplit(
                        0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
        FileInputSplit split2 =
                new FileInputSplit(
                        1,
                        split.getPath(),
                        split1.getLength(),
                        split.getLength(),
                        split.getHostnames());

        final Configuration parameters = new Configuration();

        format.configure(parameters);
        format.open(split1);

        assertThat(format.nextRecord(null)).isEqualTo("value1");
        assertThat(format.nextRecord(null)).isEqualTo("value2");
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();
        format.open(split2);

        assertThat(format.nextRecord(null)).isEqualTo("value3");
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();
    }

    /**
     * Tests that the correct number of records is read when the split boundary is exact at the
     * record boundary.
     */
    @Test
    void testReadWithBufferSizeIsMultiple() throws IOException {
        final String myString = "aaaaaaa\nbbbbbbb\nccccccc\nddddddd\n";
        final FileInputSplit split = createTempFile(myString);

        FileInputSplit split1 =
                new FileInputSplit(
                        0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
        FileInputSplit split2 =
                new FileInputSplit(
                        1,
                        split.getPath(),
                        split1.getLength(),
                        split.getLength(),
                        split.getHostnames());

        final Configuration parameters = new Configuration();

        format.setBufferSize(2 * ((int) split1.getLength()));
        format.configure(parameters);

        String next;
        int count = 0;

        // read split 1
        format.open(split1);
        while ((next = format.nextRecord(null)) != null) {
            assertThat(next).hasSize(7);
            count++;
        }
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
        format.close();

        // this one must have read one too many, because the next split will skip the trailing
        // remainder
        // which happens to be one full record
        assertThat(count).isEqualTo(3);

        // read split 2
        format.open(split2);
        while ((next = format.nextRecord(null)) != null) {
            assertThat(next).hasSize(7);
            count++;
        }
        format.close();

        assertThat(count).isEqualTo(4);
    }

    @Test
    void testReadExactlyBufferSize() throws IOException {
        final String myString = "aaaaaaa\nbbbbbbb\nccccccc\nddddddd\n";

        final FileInputSplit split = createTempFile(myString);
        final Configuration parameters = new Configuration();

        format.setBufferSize((int) split.getLength());
        format.configure(parameters);
        format.open(split);

        String next;
        int count = 0;
        while ((next = format.nextRecord(null)) != null) {
            assertThat(next).hasSize(7);
            count++;
        }
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();

        assertThat(count).isEqualTo(4);
    }

    @Test
    void testReadRecordsLargerThanBuffer() throws IOException {
        final String myString =
                "aaaaaaaaaaaaaaaaaaaaa\n"
                        + "bbbbbbbbbbbbbbbbbbbbbbbbb\n"
                        + "ccccccccccccccccccc\n"
                        + "ddddddddddddddddddddddddddddddddddd\n";

        final FileInputSplit split = createTempFile(myString);
        FileInputSplit split1 =
                new FileInputSplit(
                        0, split.getPath(), 0, split.getLength() / 2, split.getHostnames());
        FileInputSplit split2 =
                new FileInputSplit(
                        1,
                        split.getPath(),
                        split1.getLength(),
                        split.getLength(),
                        split.getHostnames());

        final Configuration parameters = new Configuration();

        format.setBufferSize(8);
        format.configure(parameters);

        String next;
        List<String> result = new ArrayList<>();

        format.open(split1);
        while ((next = format.nextRecord(null)) != null) {
            result.add(next);
        }
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
        format.close();

        format.open(split2);
        while ((next = format.nextRecord(null)) != null) {
            result.add(next);
        }
        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();
        format.close();

        assertThat(result).hasSize(4);
        assertThat(result).isEqualTo(Arrays.asList(myString.split("\n")));
    }

    @Test
    void testDelimiterOnBufferBoundary() throws IOException {

        testDelimiterOnBufferBoundary(DelimitedInputFormatTest::createTempFile);
    }

    @Test
    void testDelimiterOnBufferBoundaryWithWholeFileSplit() throws IOException {

        testDelimiterOnBufferBoundary(
                fileContent -> {
                    final FileInputSplit split = createTempFile(fileContent);
                    return new FileInputSplit(0, split.getPath(), 0, -1, split.getHostnames());
                });
    }

    private void testDelimiterOnBufferBoundary(
            FunctionWithException<String, FileInputSplit, IOException> splitCreator)
            throws IOException {
        String[] records =
                new String[] {
                    "1234567890<DEL?NO!>1234567890", "1234567890<DEL?NO!>1234567890", "<DEL?NO!>"
                };
        String delimiter = "<DELIM>";
        String fileContent = StringUtils.join(records, delimiter);

        final FileInputSplit split = splitCreator.apply(fileContent);
        final Configuration parameters = new Configuration();

        format.setBufferSize(12);
        format.setDelimiter(delimiter);
        format.configure(parameters);
        format.open(split);

        for (String record : records) {
            String value = format.nextRecord(null);
            assertThat(value).isEqualTo(record);
        }

        assertThat(format.nextRecord(null)).isNull();
        assertThat(format.reachedEnd()).isTrue();

        format.close();
    }

    // -- Statistics --//

    @Test
    void testGetStatistics() throws IOException {
        final String myString = "my mocked line 1\nmy mocked line 2\n";
        final long size = myString.length();
        final Path filePath = createTempFilePath(myString);

        final String myString2 = "my mocked line 1\nmy mocked line 2\nanother mocked line3\n";
        final long size2 = myString2.length();
        final Path filePath2 = createTempFilePath(myString2);

        final long totalSize = size + size2;

        DelimitedInputFormat<String> format = new MyTextInputFormat();
        format.setFilePaths(filePath.toUri().toString(), filePath2.toUri().toString());

        FileInputFormat.FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats).isNotNull();
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(totalSize);
    }

    @Test
    void testGetStatisticsFileDoesNotExist() throws IOException {
        DelimitedInputFormat<String> format = new MyTextInputFormat();
        format.setFilePaths(
                "file:///path/does/not/really/exist", "file:///another/path/that/does/not/exist");

        FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats).as("The file statistics should be null.").isNull();
    }

    @Test
    void testGetStatisticsSingleFileWithCachedVersion() throws IOException {
        final String myString = "my mocked line 1\nmy mocked line 2\n";
        final Path tempFile = createTempFilePath(myString);
        final long size = myString.length();
        final long fakeSize = 10065;

        DelimitedInputFormat<String> format = new MyTextInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats).isNotNull();
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(size);

        format = new MyTextInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics newStats = format.getStatistics(stats);
        assertThat(newStats).as("Statistics object was changed.").isEqualTo(stats);

        // insert fake stats with the correct modification time. the call should return the fake
        // stats
        format = new MyTextInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics fakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime(),
                        fakeSize,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics latest = format.getStatistics(fakeStats);
        assertThat(latest.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(fakeSize);

        // insert fake stats with the expired modification time. the call should return new accurate
        // stats
        format = new MyTextInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics outDatedFakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime() - 1,
                        fakeSize,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
        assertThat(reGathered.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(size);
    }

    static FileInputSplit createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        try (Writer out = new OutputStreamWriter(Files.newOutputStream(tempFile.toPath()))) {
            out.write(contents);
        }

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    static FileInputSplit createTempFile(String contents, String charset) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        try (Writer out =
                new OutputStreamWriter(Files.newOutputStream(tempFile.toPath()), charset)) {
            out.write(contents);
        }

        return new FileInputSplit(
                0,
                new Path(tempFile.toURI().toString()),
                0,
                tempFile.length(),
                new String[] {"localhost"});
    }

    protected static final class MyTextInputFormat extends DelimitedInputFormat<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String readRecord(String reuse, byte[] bytes, int offset, int numBytes) {
            return new String(bytes, offset, numBytes, ConfigConstants.DEFAULT_CHARSET);
        }

        @Override
        public boolean supportsMultiPaths() {
            return true;
        }
    }

    private static Path createTempFilePath(String contents) throws IOException {
        File tempFile = File.createTempFile("test_contents", "tmp");
        tempFile.deleteOnExit();

        try (OutputStreamWriter wrt =
                new OutputStreamWriter(Files.newOutputStream(tempFile.toPath()))) {
            wrt.write(contents);
        }
        return new Path(tempFile.toURI().toString());
    }
}
