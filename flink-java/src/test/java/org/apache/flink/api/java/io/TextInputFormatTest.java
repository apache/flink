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

import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TextInputFormat}. */
class TextInputFormatTest {

    @Test
    void testSimpleRead(@TempDir File tempDir) throws IOException {
        final String first = "First line";
        final String second = "Second line";
        File tempFile = File.createTempFile("TextInputFormatTest", "tmp", tempDir);
        tempFile.setWritable(true);
        try (PrintStream ps = new PrintStream(tempFile)) {
            ps.println(first);
            ps.println(second);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));

        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertThat(splits).as("expected at least one input split").isNotEmpty();

        inputFormat.open(splits[0]);
        try {
            assertThat(inputFormat.reachedEnd()).isFalse();
            String result = inputFormat.nextRecord("");
            assertThat(result).as("Expecting first record here").isNotNull().isEqualTo(first);

            assertThat(inputFormat.reachedEnd()).isFalse();
            result = inputFormat.nextRecord(result);
            assertThat(result).as("Expecting second record here").isNotNull().isEqualTo(second);

            assertThat(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result)).isTrue();
        } finally {
            inputFormat.close();
        }
    }

    @Test
    void testNestedFileRead(@TempDir File parentDir) throws IOException {
        String[] dirs = new String[] {"first", "second"};
        List<String> expectedFiles = new ArrayList<>();

        for (String dir : dirs) {
            // create input file
            File tmpDir = new File(parentDir, dir);
            Files.createDirectories(tmpDir.toPath());

            File tempFile = File.createTempFile("TextInputFormatTest", ".tmp", tmpDir);

            expectedFiles.add(
                    new Path(tempFile.getAbsolutePath())
                            .makeQualified(FileSystem.getLocalFileSystem())
                            .toString());
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(parentDir.toURI()));
        inputFormat.setNestedFileEnumeration(true);
        inputFormat.setNumLineSamples(10);

        // this is to check if the setter overrides the configuration (as expected)
        Configuration config = new Configuration();
        config.setBoolean("recursive.file.enumeration", false);
        config.setString("delimited-format.numSamples", "20");
        inputFormat.configure(config);

        assertThat(inputFormat.getNestedFileEnumeration()).isTrue();
        assertThat(inputFormat.getNumLineSamples()).isEqualTo(10);

        FileInputSplit[] splits = inputFormat.createInputSplits(expectedFiles.size());

        List<String> paths = new ArrayList<>();
        for (FileInputSplit split : splits) {
            paths.add(split.getPath().toString());
        }

        Collections.sort(expectedFiles);
        Collections.sort(paths);
        for (int i = 0; i < expectedFiles.size(); i++) {
            assertThat(paths.get(i)).isEqualTo(expectedFiles.get(i));
        }
    }

    /**
     * This tests cases when line ends with \r\n and \n is used as delimiter, the last \r should be
     * removed.
     */
    @Test
    void testRemovingTrailingCR(@TempDir File tmpFile) throws IOException {

        testRemovingTrailingCR(tmpFile, "\n", "\n");
        testRemovingTrailingCR(tmpFile, "\r\n", "\n");

        testRemovingTrailingCR(tmpFile, "|", "|");
        testRemovingTrailingCR(tmpFile, "|", "\n");
    }

    private void testRemovingTrailingCR(File tmpFile, String lineBreaker, String delimiter)
            throws IOException {
        String first = "First line";
        String second = "Second line";
        String content = first + lineBreaker + second + lineBreaker;

        // create input file
        File tempFile = File.createTempFile("TextInputFormatTest", "tmp", tmpFile);
        tempFile.setWritable(true);

        try (OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile))) {
            wrt.write(content);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
        inputFormat.setFilePath(tempFile.toURI().toString());

        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        inputFormat.setDelimiter(delimiter);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);

        inputFormat.open(splits[0]);

        String result;
        if ((delimiter.equals("\n") && (lineBreaker.equals("\n") || lineBreaker.equals("\r\n")))
                || (lineBreaker.equals(delimiter))) {

            result = inputFormat.nextRecord("");
            assertThat(result).as("Expecting first record here").isNotNull().isEqualTo(first);

            result = inputFormat.nextRecord(result);
            assertThat(result).as("Expecting second record here").isNotNull().isEqualTo(second);

            result = inputFormat.nextRecord(result);
            assertThat(result).as("The input file is over").isNull();

        } else {
            result = inputFormat.nextRecord("");
            assertThat(result).as("Expecting first record here").isNotNull().isEqualTo(content);
        }
    }

    @Test
    void testCompressedRead(@TempDir File tempDir) throws IOException {
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

        final String first = "First line";
        final String second = "Second line";

        // create input file
        File tempFile = File.createTempFile("TextInputFormatTest", ".compressed", tempDir);
        tempFile.setWritable(true);

        try (PrintStream ps = new PrintStream(tempFile)) {
            ps.println(first);
            ps.println(second);
        }

        TextInputFormat inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
        Configuration parameters = new Configuration();
        inputFormat.configure(parameters);

        FileInputSplit[] splits = inputFormat.createInputSplits(1);
        assertThat(splits).as("expected at least one input split").isNotEmpty();

        inputFormat.open(splits[0]);
        try {
            assertThat(inputFormat.reachedEnd()).isFalse();
            String result = inputFormat.nextRecord("");
            assertThat(result).as("Expecting first record here").isNotNull().isEqualTo(first);
            assertThat(inputFormat.reachedEnd()).isFalse();

            Long currentOffset = inputFormat.getCurrentState();
            inputFormat.close();

            inputFormat = new TextInputFormat(new Path(tempFile.toURI().toString()));
            inputFormat.configure(parameters);
            inputFormat.reopen(splits[0], currentOffset);

            assertThat(inputFormat.reachedEnd()).isFalse();
            result = inputFormat.nextRecord(result);
            assertThat(result).as("Expecting second record here").isNotNull().isEqualTo(second);

            assertThat(inputFormat.reachedEnd() || null == inputFormat.nextRecord(result)).isTrue();
        } finally {
            inputFormat.close();
        }
    }
}
